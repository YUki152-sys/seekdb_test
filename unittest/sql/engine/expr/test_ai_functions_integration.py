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
"""Run AI function SQL and HTTP contract tests against an isolated seekdb.

Requires only Python's standard library and the mysql client. Start a freshly
built seekdb, then run this file with --port PORT. No external model is used.
Creates uniquely named models/endpoints and drops them even on test failures.
"""

import argparse
import json
import re
import subprocess
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def literal(value):
    # A hex literal avoids dependence on the session's backslash escape mode.
    return "CONVERT(X'" + value.encode().hex() + "' USING utf8mb4)"


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append({"path": self.path, "body": body,
                                     "headers": dict(self.headers)})
        mode = self.server.mode
        if mode == "http_error":
            self.send_error(400, "test fixture error")
            return
        if mode == "invalid_json":
            response = b"not JSON"
        elif mode == "array_root":
            response = b"[]"
        elif mode == "null_root":
            response = b"null"
        else:
            if self.path.endswith("generate"):
                data = {"response": body.get("prompt")}
                field = "response"
            elif self.path.endswith("completions"):
                data = {"choices": [{"message": {"content": body["messages"][-1]["content"]}}]}
                field = "choices"
            else:
                dim = body.get("dimensions", 7)
                vector = [float(i) / 8 for i in range(dim)]
                if mode == "wrong_dimension":
                    vector.append(1.0)
                elif mode == "non_numeric":
                    vector[0] = "bad"
                elif mode == "null_element":
                    vector[0] = None
                elif mode == "boolean_element":
                    vector[0] = True
                elif mode == "float_overflow":
                    vector[0] = 1e100
                elif mode == "empty_vector":
                    vector = []
                if self.path.endswith("/embed"):
                    data = {"embeddings": [vector for _ in body["input"]]}
                    field = "embeddings"
                else:
                    data = {"data": [{"index": i, "embedding": vector}
                                     for i, _ in enumerate(body["input"])]}
                    field = "data"
                if mode == "wrong_count":
                    data[field] = []
            if mode == "missing_field":
                data = {}
            elif mode == "wrong_type":
                data[field] = 42
            elif mode == "null_field":
                data[field] = None
            elif mode == "empty_completion":
                if field == "choices":
                    data[field][0]["message"]["content"] = ""
                else:
                    data[field] = ""
            response = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class AIFunctionTests(unittest.TestCase):
    @classmethod
    def sql(cls, sql, error=False):
        result = subprocess.run(
            [ARGS.mysql, "--protocol=TCP", "-h", ARGS.host, "-P", str(ARGS.port),
             "-u", ARGS.user, "--default-character-set=utf8mb4", "-N", "-B", "--raw",
             "-Doceanbase"],
            input="SET ob_query_timeout=10000000;\n" + sql + ";\n",
            text=True, capture_output=True, timeout=35)
        if ARGS.sql_log:
            cls.events.append({"sql": sql, "stdout": result.stdout,
                               "stderr": result.stderr, "returncode": result.returncode,
                               "expect_error": error})
        if error:
            if result.returncode == 0 or not re.search(r"ERROR \d+", result.stderr):
                raise AssertionError(f"Expected SQL error: {sql}\n{result.stdout}{result.stderr}")
            return result.stderr.strip()
        if result.returncode:
            raise AssertionError(f"SQL failed: {sql[:500]}\n{result.stderr}")
        return result.stdout.removesuffix("\n")

    @classmethod
    def ddl(cls, operation, name, config=None):
        args = literal(name)
        if config is not None:
            args += "," + literal(json.dumps(config))
        return cls.sql(f"CALL DBMS_AI_SERVICE.{operation}({args})")

    @classmethod
    def endpoint_config(cls, model, path, provider="oLlAmA", **extra):
        return dict(ai_model_name=model, provider=provider,
                    url=f"http://127.0.0.1:{cls.server.server_port}/{path}", **extra)

    @classmethod
    def setUpClass(cls):
        cls.events = []
        cls.prefix = "ai_it_" + uuid.uuid4().hex[:10]
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), MockHandler)
        cls.server.mode = "normal"
        cls.server.requests = []
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.models = []
        cls.endpoints = []
        cls.addClassCleanup(cls.cleanup)
        for suffix, kind, path, provider in [
            ("c", "completion", "api/generate", "oLlAmA"),
            ("e", "dense_embedding", "api/embed", "OLLAMA"),
            ("oc", "completion", "v1/chat/completions", "openai"),
            ("oe", "dense_embedding", "v1/embeddings", "openai"),
        ]:
            name = cls.prefix + "_" + suffix
            setattr(cls, suffix, name)
            cls.ddl("CREATE_AI_MODEL", name, {"type": kind, "model_name": name + "_remote"})
            cls.models.append(name)
            config = cls.endpoint_config(name, path, provider)
            if provider == "openai":
                config["access_key"] = "fixture-key"
            elif suffix == "e":
                config["access_key"] = ""
            cls.ddl("CREATE_AI_MODEL_ENDPOINT", name, config)
            cls.endpoints.append(name)

    @classmethod
    def cleanup(cls):
        errors = []
        try:
            for name in reversed(cls.endpoints):
                try:
                    cls.ddl("DROP_AI_MODEL_ENDPOINT", name)
                except AssertionError as error:
                    errors.append(str(error))
            for name in reversed(cls.models):
                try:
                    cls.ddl("DROP_AI_MODEL", name)
                except AssertionError as error:
                    errors.append(str(error))
        finally:
            cls.server.shutdown()
            cls.server.server_close()
            cls.thread.join()
            if ARGS.requests_log:
                with open(ARGS.requests_log, "w", encoding="utf-8") as out:
                    for request in cls.server.requests:
                        out.write(json.dumps(request, ensure_ascii=False) + "\n")
            if ARGS.sql_log:
                with open(ARGS.sql_log, "w", encoding="utf-8") as out:
                    for event in cls.events:
                        out.write(json.dumps(event, ensure_ascii=False) + "\n")
        if errors:
            raise AssertionError("Cleanup failed: " + "\n".join(errors))

    def setUp(self):
        self.server.mode = "normal"

    def complete(self, prompt, config=None, model=None, error=False):
        args = literal(model or self.c) + "," + prompt
        if config is not None:
            args += "," + config
        output = self.sql(f"SELECT AI_COMPLETE({args})", error=error)
        return output

    def embed(self, dimension=None, model=None, error=False):
        args = literal(model or self.e) + "," + literal("向量输入")
        if dimension is not None:
            args += "," + dimension
        output = self.sql(f"SELECT AI_EMBED({args})", error=error)
        return output if error else json.loads(output)

    def assert_healthy(self):
        self.server.mode = "normal"
        self.assertEqual(self.sql("SELECT 1 + 2"), "3")
        self.assertEqual(self.complete(literal("recovery")), "recovery")

    def test_endpoint_crud_and_provider_changes(self):
        name = self.prefix + "_crud"
        model = self.prefix + "_crud_model"
        self.ddl("CREATE_AI_MODEL", model, {"type": "completion", "model_name": model + "_remote"})
        self.models.append(model)
        self.ddl("CREATE_AI_MODEL_ENDPOINT", name, self.endpoint_config(model, "api/generate"))
        self.endpoints.append(name)
        row = self.sql("SELECT provider, coalesce(length(access_key),0) FROM DBA_OB_AI_MODEL_ENDPOINTS "
                       f"WHERE endpoint_name={literal(name)}")
        self.assertEqual(row.lower(), "ollama\t0")
        self.assertEqual(self.complete(literal("created"), model=model), "created")
        self.assertEqual(self.server.requests[-1]["path"], "/api/generate")
        # Switching to an authenticated provider must require a key.
        self.sql(f"CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT({literal(name)},"
                 f"{literal(json.dumps({'provider': 'openai'}))})", error=True)
        self.ddl("ALTER_AI_MODEL_ENDPOINT", name,
                 {"provider": "openai", "access_key": "fixture-key",
                  "url": self.endpoint_config(model, "v1/chat/completions")["url"]})
        self.assertEqual(self.complete(literal("altered openai"), model=model), "altered openai")
        self.assertEqual(self.server.requests[-1]["path"], "/v1/chat/completions")
        self.assertIn("messages", self.server.requests[-1]["body"])
        self.ddl("ALTER_AI_MODEL_ENDPOINT", name,
                 {"provider": "OlLaMa", "access_key": "",
                  "url": self.endpoint_config(model, "api/generate")["url"]})
        self.assertEqual(self.complete(literal("altered ollama"), model=model), "altered ollama")
        self.assertEqual(self.server.requests[-1]["path"], "/api/generate")
        self.assertIs(self.server.requests[-1]["body"]["stream"], False)
        self.assertNotIn("authorization", {k.lower() for k in self.server.requests[-1]["headers"]})
        self.assertEqual(self.sql("SELECT lower(provider), coalesce(length(access_key),0) "
                                 f"FROM DBA_OB_AI_MODEL_ENDPOINTS WHERE endpoint_name={literal(name)}"),
                         "ollama\t0")
        self.ddl("DROP_AI_MODEL_ENDPOINT", name)
        self.endpoints.remove(name)
        self.assertEqual(self.sql("SELECT count(*) FROM DBA_OB_AI_MODEL_ENDPOINTS "
                                 f"WHERE endpoint_name={literal(name)}"), "0")
        self.ddl("DROP_AI_MODEL", model)
        self.models.remove(model)

    def test_non_ollama_requires_key(self):
        for key in ({}, {"access_key": ""}):
            with self.subTest(key=key):
                config = self.endpoint_config(self.c, "v1/chat/completions", "openai", **key)
                name = self.prefix + "_nokey"
                try:
                    self.sql(f"CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT("
                             f"{literal(name)},{literal(json.dumps(config))})", error=True)
                finally:
                    if self.sql("SELECT count(*) FROM DBA_OB_AI_MODEL_ENDPOINTS "
                                f"WHERE endpoint_name={literal(name)}") != "0":
                        self.ddl("DROP_AI_MODEL_ENDPOINT", name)

    def test_ollama_completion_wire_contract(self):
        text = '你好 "seekdb"\nUnicode: 🌊'
        self.assertEqual(self.complete(literal(text)), text)
        request = self.server.requests[-1]
        self.assertEqual(request["path"], "/api/generate")
        self.assertEqual(request["body"]["model"], self.c + "_remote")
        self.assertEqual(request["body"]["prompt"], text)
        self.assertIs(request["body"]["stream"], False)
        headers = {k.lower(): v for k, v in request["headers"].items()}
        self.assertEqual(headers["content-type"], "application/json")
        self.assertNotIn("authorization", headers)

    def test_openai_completion_wire_contract(self):
        self.assertEqual(self.complete(literal("openai"), model=self.oc), "openai")
        request = self.server.requests[-1]
        self.assertEqual(request["path"], "/v1/chat/completions")
        self.assertEqual(request["body"]["model"], self.oc + "_remote")
        self.assertEqual(request["body"]["messages"], [{"role": "user", "content": "openai"}])
        headers = {k.lower(): v for k, v in request["headers"].items()}
        self.assertEqual(headers["authorization"], "Bearer fixture-key")

    def test_request_model_override(self):
        for model in (self.c, self.oc):
            with self.subTest(model=model):
                override = model + "_override"
                self.ddl("ALTER_AI_MODEL_ENDPOINT", model, {"request_model_name": override})
                try:
                    self.assertEqual(self.complete(literal("override"), model=model), "override")
                    self.assertEqual(self.server.requests[-1]["body"]["model"], override)
                finally:
                    self.ddl("ALTER_AI_MODEL_ENDPOINT", model, {"request_model_name": ""})

    def test_completion_configs(self):
        for model in (self.c, self.oc):
            for config in (None, "NULL", literal('{"temperature":0.25,"max_tokens":37}'),
                           "JSON_OBJECT('temperature',0.25,'max_tokens',37)"):
                with self.subTest(model=model, config=config):
                    self.assertEqual(self.complete(literal("configured"), config, model), "configured")
                    body = self.server.requests[-1]["body"]
                    if config not in (None, "NULL"):
                        self.assertEqual(body["temperature"], 0.25)
                        self.assertEqual(body["max_tokens"], 37)
        self.assertEqual(self.complete(literal("forced"), literal('{"stream":true}')), "forced")
        self.assertIs(self.server.requests[-1]["body"]["stream"], False)

    def test_prompt_expansion(self):
        for prompt, expected in [
            ("AI_PROMPT('{0}/{0}/{0}', REPEAT('long',1000))", "/".join(["long" * 1000] * 3)),
            ("AI_PROMPT('outer {0}', AI_PROMPT('inner {0}','中文'))", "outer inner 中文"),
            ("AI_PROMPT('{{0}} {0}', 'x')", "{0} x"),
            ("AI_PROMPT('{0}', JSON_ARRAY('a','b'))", '["a", "b"]'),
            ("AI_PROMPT('{0}', JSON_OBJECT('a','b'))", '{"a": "b"}'),
        ]:
            with self.subTest(prompt=prompt):
                output = self.complete(prompt)
                if "JSON_" in prompt:
                    self.assertEqual(json.loads(output), json.loads(expected))
                else:
                    self.assertEqual(output, expected)

    def test_invalid_placeholders(self):
        for placeholder in ("{", "{}", "{-1}", "{x}", "{1}", "{99999999999999999999999}"):
            with self.subTest(placeholder=placeholder):
                before = len(self.server.requests)
                self.complete(f"AI_PROMPT({literal(placeholder)},'x')", error=True)
                self.assertEqual(len(self.server.requests), before)
        self.assert_healthy()

    def test_longtext_and_text(self):
        table = self.prefix + "_text"
        self.sql(f"CREATE TABLE {table}(t TEXT, l LONGTEXT)")
        try:
            text = "中文🌊" * 18000
            self.sql(f"INSERT INTO {table} VALUES ('prefix {{0}}',{literal(text)})")
            result = self.sql(f"SELECT AI_COMPLETE({literal(self.c)}, AI_PROMPT(t,l)) FROM {table}")
            self.assertEqual(result, "prefix " + text)
            result = self.sql(f"SELECT AI_COMPLETE({literal(self.c)}, l) FROM {table}")
            self.assertEqual(result, text)
        finally:
            self.sql(f"DROP TABLE {table}")

    def test_embedding_native_and_explicit_dimensions(self):
        for model in (self.e, self.oe):
            for dimension in (None, "3", "11"):
                with self.subTest(model=model, dimension=dimension):
                    result = self.embed(dimension, model)
                    size = 7 if dimension is None else int(dimension)
                    self.assertEqual(result, [i / 8 for i in range(size)])
                    request = self.server.requests[-1]
                    self.assertEqual(request["body"]["input"], ["向量输入"])
                    self.assertEqual(request["body"]["model"], model + "_remote")
                    if dimension is None:
                        self.assertNotIn("dimensions", request["body"])
                    else:
                        self.assertEqual(request["body"]["dimensions"], size)

    def test_invalid_dimensions(self):
        for dimension in ("0", "-1", "1.5", "'3'", "NULL"):
            with self.subTest(dimension=dimension):
                before = len(self.server.requests)
                self.embed(dimension, error=True)
                self.assertEqual(len(self.server.requests), before)
        self.assert_healthy()

    def test_embedding_table_rows(self):
        table = self.prefix + "_rows"
        self.sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, content VARCHAR(20), dim INT)")
        try:
            self.sql(f"INSERT INTO {table} VALUES (1,'first',2),(2,'second',3),(3,'third',5)")
            before = len(self.server.requests)
            output = self.sql(f"SELECT id,AI_EMBED({literal(self.e)},content,dim) "
                              f"FROM {table} ORDER BY id")
            rows = [line.split("\t", 1) for line in output.splitlines()]
            self.assertEqual([int(row[0]) for row in rows], [1, 2, 3])
            for row, dim in zip(rows, (2, 3, 5)):
                self.assertEqual(json.loads(row[1]), [i / 8 for i in range(dim)])
            requests = self.server.requests[before:]
            self.assertEqual([(r["body"]["input"], r["body"]["dimensions"]) for r in requests],
                             [(["first"], 2), (["second"], 3), (["third"], 5)])
        finally:
            self.sql(f"DROP TABLE {table}")

    def test_invalid_completion_config(self):
        for config in ("JSON_ARRAY(1,2)", "JSON_EXTRACT('null','$')", literal("[]"),
                       literal("invalid JSON")):
            with self.subTest(config=config):
                before = len(self.server.requests)
                self.complete(literal("config"), config, error=True)
                self.assertEqual(len(self.server.requests), before)
        self.assert_healthy()

    def test_network_failure_and_recovery(self):
        # Reserve a port without listening, so connection refusal is deterministic.
        import socket
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            url = f"http://127.0.0.1:{reserved.getsockname()[1]}/api/generate"
            self.ddl("ALTER_AI_MODEL_ENDPOINT", self.c, {"url": url})
            try:
                self.complete(literal("unreachable"), error=True)
            finally:
                self.ddl("ALTER_AI_MODEL_ENDPOINT", self.c,
                         {"url": self.endpoint_config(self.c, "api/generate")["url"]})
        self.assert_healthy()

    def test_completion_bad_responses(self):
        for model in (self.c, self.oc):
            for mode in ("missing_field", "wrong_type", "null_field", "empty_completion",
                         "invalid_json", "http_error", "array_root", "null_root"):
                with self.subTest(model=model, mode=mode):
                    self.server.mode = mode
                    self.complete(literal("bad response"), model=model, error=True)
                    self.assert_healthy()

    def test_embedding_bad_responses(self):
        for model in (self.e, self.oe):
            for mode in ("missing_field", "wrong_type", "null_field", "wrong_dimension",
                         "non_numeric", "null_element", "boolean_element", "float_overflow",
                         "empty_vector", "wrong_count", "array_root", "null_root"):
                with self.subTest(model=model, mode=mode):
                    self.server.mode = mode
                    self.embed("3", model=model, error=True)
                    self.assert_healthy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--requests-log", help="Write captured fixture HTTP requests as JSONL")
    parser.add_argument("--sql-log", help="Write SQL input, output and errors as JSONL")
    ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
