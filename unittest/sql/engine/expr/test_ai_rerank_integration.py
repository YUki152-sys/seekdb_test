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
"""Verify AI_RERANK SQL values, batching and HTTP contracts using a local Mock."""

import argparse
import json
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import test_ai_functions_integration as ai

literal = ai.literal


class RerankHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append({"path": self.path, "body": body,
                                     "headers": dict(self.headers)})
        inputs = body.get("input", body)
        docs = inputs["documents"]
        mode = self.server.mode
        # Distinct scores depend on the document, independent of batch boundaries.
        results = [{"index": i, "relevance_score": int(doc.split(":", 1)[0]) % 7,
                    "document": {"text": doc}} for i, doc in enumerate(docs)]
        if mode == "sorted":
            results.sort(key=lambda row: -row["relevance_score"])
        else:
            results.reverse()  # Providers may return unsorted and tied results.
        if mode == "missing":
            results.pop()
        elif mode == "extra":
            results.append(results[0].copy())
        elif mode == "duplicate":
            results[0]["index"] = results[1]["index"]
        elif mode in ("negative", "out_of_range", "float_index", "string_index",
                      "null_index", "bool_index", "huge_index"):
            results[0]["index"] = {
                "negative": -1, "out_of_range": len(docs), "float_index": 0.5,
                "string_index": "0", "null_index": None, "bool_index": True,
                "huge_index": 18446744073709551615}[mode]
        elif mode == "missing_index":
            del results[0]["index"]
        elif mode == "missing_score":
            del results[0]["relevance_score"]
        elif mode in ("string_score", "null_score", "bool_score", "object_score"):
            results[0]["relevance_score"] = {
                "string_score": "1", "null_score": None,
                "bool_score": True, "object_score": {}}[mode]
        elif mode == "scalar_result":
            results[0] = 1
        elif mode == "null_result":
            results[0] = None
        elif mode == "double_scores":
            for row in results:
                row["relevance_score"] /= 8.0
        elif mode == "all_ties":
            for row in results:
                row["relevance_score"] = 0.25
        elif mode == "overflow_score":
            results[0]["relevance_score"] = "__overflow__"
        elif mode == "second_batch" and docs[0].startswith("20:"):
            results.pop()
        if mode == "http_error":
            self.send_error(400, "fixture failure")
            return
        data = {"results": results}
        if mode == "wrong_results":
            data["results"] = {}
        elif mode == "null_results":
            data["results"] = None
        elif mode == "missing_results":
            data = {}
        if self.path == "/dashscope":
            data = {"output": data}
            if mode == "wrong_output":
                data["output"] = []
            elif mode == "null_output":
                data["output"] = None
        if mode == "array_root":
            data = []
        response = json.dumps(data, ensure_ascii=False).encode()
        if mode == "invalid_json":
            response = b"not json"
        elif mode == "overflow_score":
            response = response.replace(b'"__overflow__"', b'1e999')
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class RerankTests(unittest.TestCase):
    # Share the SQL client, DDL and failure-safe cleanup with the other AI suite.
    ddl = ai.AIFunctionTests.__dict__["ddl"]
    cleanup = ai.AIFunctionTests.__dict__["cleanup"]

    @classmethod
    def sql(cls, sql, error=False):
        output = ai.AIFunctionTests.sql.__func__(cls, sql, error=error)
        if error and any(f"ERROR {code}" in output for code in (2002, 2003, 2006, 2013)):
            raise AssertionError("Connection failure cannot count as an expected SQL error: " + output)
        return output

    @classmethod
    def setUpClass(cls):
        cls.events = []
        cls.prefix = "rerank_it_" + uuid.uuid4().hex[:10]
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), RerankHandler)
        cls.server.mode = "normal"
        cls.server.requests = []
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.models = []
        cls.endpoints = []
        cls.addClassCleanup(cls.cleanup)
        for suffix, provider, path in [("s", "siliconflow", "rerank"),
                                       ("c", "cohere", "rerank"),
                                       ("d", "aliyun-dashscope", "dashscope")]:
            name = cls.prefix + "_" + suffix
            setattr(cls, suffix, name)
            cls.ddl("CREATE_AI_MODEL", name, {"type": "rerank", "model_name": name + "_remote"})
            cls.models.append(name)
            cls.ddl("CREATE_AI_MODEL_ENDPOINT", name,
                    {"ai_model_name": name, "provider": provider, "access_key": "fixture-key",
                     "url": f"http://127.0.0.1:{cls.server.server_port}/{path}"})
            cls.endpoints.append(name)

    def setUp(self):
        self.server.mode = "normal"

    @staticmethod
    def documents(count, objects=False):
        texts = [f"{i}:文档 {i} \\\"\n🙂" for i in range(count)]
        return ([{"id": f"id-{i}", "body": text, "meta": {"order": i},
                  "_model_score": -999} for i, text in enumerate(texts)] if objects else texts)

    def rerank(self, docs, model=None, key=None, native=False, error=False):
        doc_sql = literal(json.dumps(docs, ensure_ascii=False))
        if native:
            doc_sql = f"JSON_EXTRACT({doc_sql}, '$')"
        args = f"{literal(model or self.s)},{literal('检索 query🙂')},{doc_sql}"
        if key is not None:
            args += "," + key
        output = self.sql(f"SELECT AI_RERANK({args})", error=error)
        return output if error else json.loads(output)

    def check_ranking(self, count, objects=False, model=None, mode="normal", native=False):
        self.server.mode = mode
        docs = self.documents(count, objects)
        start = len(self.server.requests)
        actual = self.rerank(docs, model=model, key=literal("body") if objects else None, native=native)
        expected = sorted(range(count), key=lambda i: -(i % 7)) if mode != "all_ties" else list(range(count))
        self.assertEqual(len(actual), count)
        if objects:
            for row, index in zip(actual, expected):
                self.assertEqual(row, dict(docs[index], _model_score=(0.25 if mode == "all_ties" else
                                  (index % 7) / 8.0 if mode == "double_scores" else index % 7)))
        else:
            self.assertEqual([row["index"] for row in actual], expected)
            self.assertEqual([row["document"]["text"] for row in actual], [docs[i] for i in expected])
        requests = self.server.requests[start:]
        self.assertEqual(len(requests), (count + 19) // 20)
        for offset, request in enumerate(requests):
            self.assertEqual(request["path"], "/dashscope" if model == self.d else "/rerank")
            headers = {key.lower(): value for key, value in request["headers"].items()}
            self.assertEqual(headers["content-type"], "application/json")
            self.assertEqual(headers["authorization"], "Bearer fixture-key")
            body = request["body"]
            self.assertEqual(body["model"], (model or self.s) + "_remote")
            inputs = body.get("input", body)
            batch = docs[offset * 20:(offset + 1) * 20]
            self.assertEqual(inputs["documents"], [row["body"] for row in batch] if objects else batch)
            self.assertEqual(inputs["query"], "检索 query🙂")
            config = body.get("parameters", body)
            self.assertEqual(config["top_n"], len(batch))
            self.assertIs(config["return_documents"], True)

    def test_strings_sorted_unequal_batches(self):
        self.check_ranking(45, mode="sorted")

    def test_strings_unsorted(self):
        self.check_ranking(9)

    def test_object_batches(self):
        self.check_ranking(45, objects=True)

    def test_boundaries_and_native_json(self):
        for count in (1, 19, 20, 21, 40, 41, 65):
            for objects in (False, True):
                with self.subTest(count=count, objects=objects):
                    self.check_ranking(count, objects, native=True)

    def test_stable_ties(self):
        for objects in (False, True):
            self.check_ranking(45, objects, mode="all_ties")

    def test_provider_protocols_and_numeric_scores(self):
        for model in (self.s, self.c, self.d):
            for objects in (False, True):
                self.check_ranking(21, objects, model=model, mode="double_scores")

    def test_null_key(self):
        self.assertEqual(self.rerank(self.documents(3), key="NULL"), self.rerank(self.documents(3)))

    def test_text_field_selection_and_original_ids(self):
        docs = [{"id": 100, "正文🙂": "1:相同", "body": "6:另一字段"},
                {"id": None, "正文🙂": "1:相同", "body": "0:另一个值"},
                {"id": {"nested": [1, 2]}, "正文🙂": "2:不同", "body": "3:其他"}]
        for key, expected in [("正文🙂", [2, 0, 1]), ("body", [0, 2, 1])]:
            with self.subTest(key=key):
                actual = self.rerank(docs, key=literal(key), native=True)
                self.assertEqual(actual, [dict(docs[i], _model_score=int(docs[i][key].split(":")[0]))
                                          for i in expected])
                self.assertEqual(self.server.requests[-1]["body"]["documents"], [row[key] for row in docs])

    def test_table_rows_and_input_preservation(self):
        table = self.prefix + "_docs"
        self.sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, docs JSON, doc_key VARCHAR(30))")
        self.addCleanup(self.sql, f"DROP TABLE IF EXISTS {table}")
        docs = self.documents(21, objects=True)
        encoded = literal(json.dumps(docs, ensure_ascii=False))
        self.sql(f"INSERT INTO {table} VALUES (1,{encoded},'body'),(2,{encoded},'body')")
        rows = self.sql(f"SELECT id,AI_RERANK({literal(self.s)},'检索 query🙂',docs,doc_key) "
                        f"FROM {table} ORDER BY id").splitlines()
        expected = sorted(range(21), key=lambda i: -(i % 7))
        self.assertEqual(len(rows), 2)
        for row_id, row in enumerate(rows, 1):
            actual_id, result = row.split("\t", 1)
            self.assertEqual(actual_id, str(row_id))
            self.assertEqual(json.loads(result), [dict(docs[i], _model_score=i % 7) for i in expected])
        for row in self.sql(f"SELECT docs FROM {table} ORDER BY id").splitlines():
            self.assertEqual(json.loads(row), docs)

    def test_invalid_inputs(self):
        start = len(self.server.requests)
        for docs, key in [([], None), ({"body": "0:x"}, None), ([1], None),
                          ([None], None), (["0:x", {}], None),
                          ([{"id": 1}], literal("body")),
                          ([{"body": 1}], literal("body")),
                          ([{"body": None}], literal("body")),
                          ([{"body": "0:x"}], literal("")),
                          ([{"body": "0:x"}], "JSON_OBJECT('key','body')"),
                          ([{"body": "0:x"}], "1")]:
            with self.subTest(docs=docs, key=key):
                self.rerank(docs, key=key, error=True)
        self.sql(f"SELECT AI_RERANK({literal(self.s)}, 'query', NULL)", error=True)
        self.assertEqual(len(self.server.requests), start)
        self.assertEqual(self.sql("SELECT 1+2"), "3")

    def test_bad_responses_and_recovery(self):
        modes = ("missing", "extra", "duplicate", "negative", "out_of_range", "float_index",
                 "string_index", "null_index", "bool_index", "huge_index", "missing_index",
                 "missing_score", "string_score", "null_score", "bool_score", "object_score",
                 "scalar_result", "null_result", "wrong_results", "null_results", "missing_results",
                 "invalid_json", "array_root", "http_error", "overflow_score")
        for model in (self.s, self.d):
            for objects in (False, True):
                for mode in modes + (("wrong_output", "null_output") if model == self.d else ()):
                    with self.subTest(model=model, objects=objects, mode=mode):
                        self.server.mode = mode
                        self.rerank(self.documents(3, objects), model=model,
                                    key=literal("body") if objects else None, error=True)
                        self.assertEqual(self.sql("SELECT 1+2"), "3")
                        self.check_ranking(3, objects, model=model)

    def test_second_batch_failure(self):
        for objects in (False, True):
            self.server.mode = "second_batch"
            start = len(self.server.requests)
            self.rerank(self.documents(45, objects), key=literal("body") if objects else None, error=True)
            self.assertEqual(len(self.server.requests) - start, 2)
            self.check_ranking(21, objects)

    def test_connection_failure_and_recovery(self):
        import socket
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            self.ddl("ALTER_AI_MODEL_ENDPOINT", self.s,
                     {"url": f"http://127.0.0.1:{reserved.getsockname()[1]}/rerank"})
            try:
                self.rerank(self.documents(3), error=True)
            finally:
                self.ddl("ALTER_AI_MODEL_ENDPOINT", self.s,
                         {"url": f"http://127.0.0.1:{self.server.server_port}/rerank"})
        self.assertEqual(self.sql("SELECT 1+2"), "3")
        self.check_ranking(21)

    def test_wrong_model_type(self):
        name = self.prefix + "_completion"
        self.ddl("CREATE_AI_MODEL", name, {"type": "completion", "model_name": name + "_remote"})
        self.models.append(name)
        start = len(self.server.requests)
        for objects in (False, True):
            self.rerank(self.documents(3, objects), model=name,
                        key=literal("body") if objects else None, error=True)
        self.assertEqual(len(self.server.requests), start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--requests-log")
    parser.add_argument("--sql-log")
    ai.ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
