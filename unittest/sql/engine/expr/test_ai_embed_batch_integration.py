#!/usr/bin/env python3
# Copyright (c) 2025 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Assert AI_EMBED cross-row SQL/HTTP batching, row mapping and failure behavior."""

import argparse
import json
import socket
import threading
import unittest
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import test_ai_functions_integration as ai

literal = ai.literal


class EmbeddingHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append({"path": self.path, "body": body,
                                     "headers": dict(self.headers)})
        inputs = body["input"]
        if isinstance(inputs, dict):
            inputs = inputs["texts"]
        dimension = body.get("dimensions", body.get("parameters", {}).get("dimension", 7))
        mode = self.server.mode
        vectors = [[int(text.split(":", 1)[0]) + i / 8 for i in range(dimension)] for text in inputs]
        indexed = self.path != "/api/embed"
        index_key = "text_index" if self.path == "/dashscope" else "index"
        results = [{index_key: i, "embedding": vector} for i, vector in enumerate(vectors)]
        results.reverse()
        if mode == "second_batch" and any(text.startswith("20:") for text in inputs):
            results.pop()
        elif mode == "missing":
            results.pop()
        elif mode == "extra":
            results.append(results[0].copy())
        elif mode == "duplicate" and len(results) > 1:
            results[0][index_key] = results[1][index_key]
        elif mode in ("negative", "out_of_range", "huge", "float", "string", "null", "boolean"):
            results[0][index_key] = {"negative": -1, "out_of_range": len(results),
                                    "huge": 18446744073709551615, "float": 0.5,
                                    "string": "0", "null": None, "boolean": True}[mode]
        elif mode == "missing_index":
            del results[0][index_key]
        elif mode == "bad_element":
            results[0]["embedding"][0] = "bad"
        elif mode == "wrong_dimension":
            results[0]["embedding"].append(1)
        elif mode == "scalar_item":
            results[0] = 123
        elif mode == "null_vector":
            results[0]["embedding"] = None
        if mode == "http_error":
            self.send_error(400, "fixture failure")
            return
        if not indexed:
            # Ollama's protocol is positional and does not provide indices.
            data = {"embeddings": vectors}
            if mode == "missing":
                data["embeddings"].pop()
            elif mode == "bad_element":
                data["embeddings"][0][0] = "bad"
        elif self.path == "/dashscope":
            data = {"output": {"embeddings": results}}
        else:
            data = {"data": results}
        response = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class EmbeddingBatchTests(unittest.TestCase):
    ddl = ai.AIFunctionTests.__dict__["ddl"]
    cleanup = ai.AIFunctionTests.__dict__["cleanup"]

    @classmethod
    def sql(cls, sql, error=False):
        output = ai.AIFunctionTests.sql.__func__(cls, sql, error=error)
        if error and any(f"ERROR {code}" in output for code in (2002, 2003, 2006, 2013)):
            raise AssertionError("Connection failure cannot count as a SQL error: " + output)
        return output

    @classmethod
    def setUpClass(cls):
        cls.events = []
        cls.prefix = "embed_batch_it_" + uuid.uuid4().hex[:8]
        cls.models = []
        cls.endpoints = []
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), EmbeddingHandler)
        cls.server.requests = []
        cls.server.mode = "normal"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.addClassCleanup(cls.cleanup)
        for suffix, provider, path in [("o", "openai", "v1/embeddings"),
                                       ("d", "aliyun-dashscope", "dashscope"),
                                       ("l", "ollama", "api/embed")]:
            name = cls.prefix + "_" + suffix
            setattr(cls, suffix, name)
            cls.ddl("CREATE_AI_MODEL", name, {"type": "dense_embedding", "model_name": name + "_remote"})
            cls.models.append(name)
            config = {"ai_model_name": name, "provider": provider,
                      "url": f"http://127.0.0.1:{cls.server.server_port}/{path}"}
            if provider != "ollama":
                config["access_key"] = "fixture-key"
            cls.ddl("CREATE_AI_MODEL_ENDPOINT", name, config)
            cls.endpoints.append(name)
        cls.table = cls.prefix + "_rows"
        cls.sql(f"CREATE TABLE {cls.table}(id INT PRIMARY KEY, content LONGTEXT, dim INT, model VARCHAR(100))")
        cls.addClassCleanup(cls.sql, f"DROP TABLE IF EXISTS {cls.table}")
        values = [f"({i},{literal(str(i) + ':中文🙂 ' + ('长' * 5000 if i == 33 else ''))},"
                  f"{3 + i % 2},{literal(cls.o if i < 35 else cls.l)})" for i in range(85)]
        cls.sql(f"INSERT INTO {cls.table} VALUES " + ",".join(values))

    def setUp(self):
        self.server.mode = "normal"

    def query(self, model=None, dimension="3", content="content", tail="", error=False):
        args = f"{literal(model or self.o)},{content}"
        if dimension is not None:
            args += "," + dimension
        start = len(self.server.requests)
        output = self.sql(f"SELECT id,AI_EMBED({args}) FROM {self.table} {tail}", error=error)
        rows = [] if error or not output else [(int(row.split("\t", 1)[0]),
                  None if row.split("\t", 1)[1] == "NULL" else json.loads(row.split("\t", 1)[1]))
                  for row in output.splitlines()]
        return rows, self.server.requests[start:]

    def assert_rows(self, rows, ids, dimension=3, null_ids=()):
        self.assertEqual([row[0] for row in rows], list(ids))
        for row_id, vector in rows:
            self.assertEqual(vector, None if row_id in null_ids else
                             [row_id + i / 8 for i in range(dimension(row_id) if callable(dimension) else dimension)])

    def assert_requests(self, requests, ids, batched=True):
        ids = list(ids)
        texts = []
        for request in requests:
            body = request["body"]
            inputs = body["input"]
            if isinstance(inputs, dict):
                inputs = inputs["texts"]
            self.assertLessEqual(len(inputs), 20)
            self.assertGreater(len(inputs), 0)
            for text in inputs:
                row_id = int(text.split(":", 1)[0])
                expected = (f"{row_id}:多批次" if row_id >= 1000 else
                            f"{row_id}:中文🙂 " + ("长" * 5000 if row_id == 33 else ""))
                self.assertEqual(text, expected)
            texts.extend(int(text.split(":", 1)[0]) for text in inputs)
            model = {"/v1/embeddings": self.o, "/dashscope": self.d, "/api/embed": self.l}[request["path"]]
            self.assertEqual(body["model"], model + "_remote")
            headers = {key.lower(): value for key, value in request["headers"].items()}
            self.assertEqual(headers["content-type"], "application/json")
            if model == self.l:
                self.assertNotIn("authorization", headers)
            else:
                self.assertEqual(headers["authorization"], "Bearer fixture-key")
        self.assertEqual(Counter(texts), Counter(ids))
        if batched and len(ids) > 1:
            self.assertLess(len(requests), len(ids))
            self.assertTrue(any(len(request["body"]["input"].get("texts", [])) > 1
                                if isinstance(request["body"]["input"], dict)
                                else len(request["body"]["input"]) > 1 for request in requests))

    def test_table_scan_batches_and_shuffled_results(self):
        rows, requests = self.query(tail="ORDER BY id")
        self.assert_rows(rows, range(85))
        self.assert_requests(requests, range(85))
        self.assertEqual(len(requests), 5)

    def test_null_propagation(self):
        rows, requests = self.query(content="CASE WHEN id % 4=0 THEN NULL ELSE content END", tail="ORDER BY id")
        self.assert_rows(rows, range(85), null_ids=range(0, 85, 4))
        self.assert_requests(requests, [i for i in range(85) if i % 4])
        rows, requests = self.query(content="NULL", tail="WHERE id < 5 ORDER BY id")
        self.assert_rows(rows, range(5), null_ids=range(5))
        self.assertEqual(requests, [])
        value = self.sql(f"SELECT AI_EMBED({literal(self.o)},NULL),AI_EMBED(NULL,'0:x')")
        self.assertEqual(value, "NULL\tNULL")
        start = len(self.server.requests)
        output = self.sql(f"SELECT id,AI_EMBED(CASE WHEN id%3=0 THEN NULL ELSE {literal(self.o)} END,"
                          f"content,3) FROM {self.table} WHERE id<21 ORDER BY id")
        rows = [(int(row.split('\t')[0]), None if row.split('\t')[1] == 'NULL'
                 else json.loads(row.split('\t')[1])) for row in output.splitlines()]
        self.assert_rows(rows, range(21), null_ids=range(0, 21, 3))
        self.assert_requests(self.server.requests[start:], [i for i in range(21) if i%3])

    def test_filter_limit_and_empty(self):
        for tail, ids in [("WHERE id % 3 = 1 ORDER BY id", list(range(1,85,3))),
                          ("WHERE id >= 8 ORDER BY id LIMIT 7", list(range(8,15))),
                          ("ORDER BY id LIMIT 5 OFFSET 7", list(range(7,12))),
                          ("WHERE id < 0", []), ("LIMIT 0", [])]:
            with self.subTest(tail=tail):
                rows, requests = self.query(tail=tail)
                self.assert_rows(rows, ids)
                # LIMIT may be above projection: any prefetched input must still be valid.
                if not ids:
                    self.assertEqual(requests, [])
                elif "LIMIT" not in tail:
                    self.assert_requests(requests, ids)
                else:
                    sent = [int(text.split(":",1)[0]) for r in requests for text in r["body"]["input"]]
                    self.assertTrue(set(ids) <= set(sent))
                    self.assertTrue(set(sent) <= set(range(8,85) if "id >= 8" in tail else range(85)))

    def test_native_and_row_dimensions(self):
        for model in (self.o, self.d, self.l):
            with self.subTest(model=model):
                rows, requests = self.query(model=model, dimension=None, tail="WHERE id < 21 ORDER BY id")
                self.assert_rows(rows, range(21), dimension=7)
                self.assert_requests(requests, range(21))
                for request in requests:
                    self.assertNotIn("dimensions", request["body"])
                rows, requests = self.query(model=model, dimension="dim", tail="WHERE id < 21 ORDER BY id")
                self.assert_rows(rows, range(21), dimension=lambda i:3+i%2)
                self.assert_requests(requests, range(21), batched=False)

    def test_case_short_circuit_and_repeated_expression(self):
        start = len(self.server.requests)
        rows = self.sql(f"SELECT id, CASE WHEN id%2=0 THEN NULL ELSE AI_EMBED({literal(self.o)},content,3) END "
                        f"FROM {self.table} WHERE id<12 ORDER BY id")
        actual = [(int(row.split('\t')[0]), None if row.split('\t')[1]=='NULL' else json.loads(row.split('\t')[1]))
                  for row in rows.splitlines()]
        self.assert_rows(actual, range(12), null_ids=range(0,12,2))
        self.assert_requests(self.server.requests[start:], range(1,12,2))
        start=len(self.server.requests)
        rows=self.sql(f"SELECT id,AI_EMBED({literal(self.o)},content,3),AI_EMBED({literal(self.o)},content,3) "
                      f"FROM {self.table} WHERE id<21 ORDER BY id")
        for row in rows.splitlines():
            row_id,a,b=row.split('\t')
            self.assertEqual(json.loads(a),[int(row_id)+i/8 for i in range(3)])
            self.assertEqual(a,b)
        self.assertLess(len(self.server.requests)-start,21)

    def test_mixed_models(self):
        rows=self.sql(f"SELECT id,AI_EMBED(model,content,3) FROM {self.table} ORDER BY id")
        self.assert_rows([(int(row.split('\t')[0]),json.loads(row.split('\t')[1])) for row in rows.splitlines()],range(85))

    def test_invalid_responses_and_recovery(self):
        for model in (self.o,self.d):
            for mode in ("missing","extra","duplicate","negative","out_of_range","huge","float","string",
                         "null","boolean","missing_index","bad_element","wrong_dimension","scalar_item","null_vector","http_error"):
                with self.subTest(model=model,mode=mode):
                    self.server.mode=mode
                    self.query(model=model,tail="WHERE id<5 ORDER BY id",error=True)
                    self.assertEqual(self.sql('SELECT 1+2'),'3')
                    self.server.mode='normal'
                    rows,_=self.query(model=model,tail="WHERE id<5 ORDER BY id")
                    self.assert_rows(rows,range(5))

    def test_failed_second_batch_and_recovery(self):
        self.server.mode='second_batch'
        _, failed_requests = self.query(tail='ORDER BY id',error=True)
        self.assertEqual(len(failed_requests), 2)
        self.server.mode='normal'
        rows,requests=self.query(tail='ORDER BY id')
        self.assert_rows(rows,range(85))
        self.assert_requests(requests,range(85))

    def test_scalar_execution(self):
        for model in (self.o,self.d,self.l):
            output=self.sql(f"SELECT AI_EMBED({literal(model)},'12:标量',3)")
            self.assertEqual(json.loads(output),[12+i/8 for i in range(3)])
        rows,_=self.query(tail='WHERE id<5 ORDER BY id')
        self.assert_rows(rows,range(5))

    def test_multiple_executor_batches(self):
        ids = range(1000, 2150)
        self.addCleanup(self.sql, f"DELETE FROM {self.table} WHERE id >= 1000")
        for start in range(1000, 2150, 250):
            values = [f"({i},{literal(str(i) + ':多批次')},3,{literal(self.o)})"
                      for i in range(start, min(start + 250, 2150))]
            self.sql(f"INSERT INTO {self.table} VALUES " + ",".join(values))
        rows, requests = self.query(tail="WHERE id >= 1000 ORDER BY id")
        self.assert_rows(rows, ids)
        self.assert_requests(requests, ids)
        self.assertGreater(len(requests), 50)

    def test_batch_boundaries_and_group_dimensions(self):
        for count in (1, 19, 20, 21, 40, 41):
            with self.subTest(count=count):
                rows, requests = self.query(tail=f"WHERE id<{count} ORDER BY id")
                self.assert_rows(rows, range(count))
                self.assert_requests(requests, range(count))
                # Executor batches can be smaller than the HTTP input limit.
                self.assertGreaterEqual(len(requests), (count + 19)//20)
        rows, requests = self.query(dimension="CASE WHEN id<10 THEN 3 ELSE 4 END",
                                    tail="WHERE id<21 ORDER BY id")
        self.assert_rows(rows, range(21), dimension=lambda i: 3 if i<10 else 4)
        self.assertEqual([(len(r['body']['input']),r['body']['dimensions']) for r in requests],
                         [(10,3),(11,4)])

    def test_invalid_row_inputs(self):
        for content, dimension in [("CASE WHEN id=2 THEN '' ELSE content END", "3"),
                                   ("content", "CASE WHEN id=2 THEN 0 ELSE 3 END"),
                                   ("content", "CASE WHEN id=2 THEN -1 ELSE 3 END"),
                                   ("content", "CASE WHEN id=2 THEN NULL ELSE 3 END")]:
            with self.subTest(content=content, dimension=dimension):
                self.query(content=content, dimension=dimension, tail="WHERE id<5 ORDER BY id", error=True)
                self.assertEqual(self.sql("SELECT 1+2"), "3")
                rows, _ = self.query(tail="WHERE id<5 ORDER BY id")
                self.assert_rows(rows, range(5))

    def test_network_failure_and_recovery(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            self.ddl("ALTER_AI_MODEL_ENDPOINT", self.o,
                     {"url": f"http://127.0.0.1:{reserved.getsockname()[1]}/v1/embeddings"})
            try:
                self.query(tail="WHERE id<5 ORDER BY id", error=True)
            finally:
                self.ddl("ALTER_AI_MODEL_ENDPOINT", self.o,
                         {"url": f"http://127.0.0.1:{self.server.server_port}/v1/embeddings"})
        rows, requests = self.query(tail="WHERE id<5 ORDER BY id")
        self.assert_rows(rows, range(5))
        self.assert_requests(requests, range(5))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,required=True)
    parser.add_argument('--user',default='root')
    parser.add_argument('--mysql',default='mysql')
    parser.add_argument('--requests-log')
    parser.add_argument('--sql-log')
    ai.ARGS,remaining=parser.parse_known_args()
    unittest.main(argv=[__file__,*remaining],verbosity=2)
