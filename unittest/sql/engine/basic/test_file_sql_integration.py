#!/usr/bin/env python3
# Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0.
"""Check file tables using actual seekdb SQL, without an external query engine.

Start an isolated seekdb and pass --port/--host or --socket. The test changes
secure_file_priv on that instance. All input files live in a temporary directory.
Parquet fixtures use the published Compact Protocol and PLAIN page formats.
"""
import argparse
import base64
import csv
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import threading
import unittest


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result) + bytes([value])


def integer(value):
    return varint((value << 1) ^ (value >> 63))


def binary(value):
    value = value.encode() if isinstance(value, str) else value
    return varint(len(value)) + value


def compact(fields):
    result, previous = bytearray(), 0
    for number, kind, value in fields:
        delta = number - previous
        result += bytes([(delta << 4) | kind]) if 0 < delta < 16 else bytes([kind]) + integer(number)
        result += value
        previous = number
    return bytes(result) + b"\0"


def sequence(kind, values):
    count = len(values)
    return (bytes([(min(count, 15) << 4) | kind]) + (varint(count) if count >= 15 else b"")
            + b"".join(values))


def parquet(path, rows, *, nullable=False, snappy=False, dictionary=False, groups=2):
    """Write a standard single INT64 column fixture, with distinct row groups."""
    data, rowgroups = bytearray(b"PAR1"), []
    for start in range(0, len(rows), groups):
        group = rows[start:start + groups]
        offset = len(data)
        levels = b""
        if nullable:
            levels = b"".join(varint(2) + bytes([value is not None]) for value in group)
            levels = struct.pack("<I", len(levels)) + levels
        values = [v for v in group if v is not None]
        dictionary_offset = None
        if dictionary:
            unique = sorted(set(values))
            body = b"".join(struct.pack("<q", v) for v in unique)
            encoded = (varint(len(body)) + (bytes([(len(body) - 1) << 2]) + body if body else b"")
                       if snappy else body)
            detail = compact([(1, 5, integer(len(unique))), (2, 5, integer(0))])
            data += compact([(1, 5, integer(2)), (2, 5, integer(len(body))),
                             (3, 5, integer(len(encoded))), (7, 12, detail)]) + encoded
            dictionary_offset = offset
            offset = len(data)
            width = max(1, (len(unique) - 1).bit_length())
            value_bytes = bytes([width]) + b"".join(varint(2) + bytes([unique.index(v)]) for v in values)
        else:
            value_bytes = b"".join(struct.pack("<q", v) for v in values)
        body = levels + value_bytes
        encoded = varint(len(body)) + bytes([(len(body) - 1) << 2]) + body if snappy else body
        detail = compact([(1, 5, integer(len(group))), (2, 5, integer(8 if dictionary else 0)),
                          (3, 5, integer(3)), (4, 5, integer(3))])
        data += compact([(1, 5, integer(0)), (2, 5, integer(len(body))),
                         (3, 5, integer(len(encoded))), (5, 12, detail)]) + encoded
        first = dictionary_offset if dictionary else offset
        metadata = [(1, 5, integer(2)), (2, 9, sequence(5, [integer(0), integer(3)])),
                    (3, 9, sequence(8, [binary("n")])), (4, 5, integer(1 if snappy else 0)),
                    (5, 6, integer(len(group))), (6, 6, integer(len(body))),
                    (7, 6, integer(len(data) - first)), (9, 6, integer(offset))]
        if dictionary:
            metadata.append((11, 6, integer(dictionary_offset)))
        chunk = compact([(2, 6, integer(first)), (3, 12, compact(metadata))])
        rowgroups.append(compact([(1, 9, sequence(12, [chunk])),
                                  (2, 6, integer(len(body))), (3, 6, integer(len(group)))]))
    root = compact([(4, 8, binary("schema")), (5, 5, integer(1))])
    column = compact([(1, 5, integer(2)), (3, 5, integer(1 if nullable else 0)), (4, 8, binary("n"))])
    footer = compact([(1, 5, integer(1)), (2, 9, sequence(12, [root, column])),
                      (3, 6, integer(len(rows))), (4, 9, sequence(12, rowgroups))])
    path.write_bytes(data + footer + struct.pack("<I", len(footer)) + b"PAR1")


class FileSQLTests(unittest.TestCase):
    events = []

    @classmethod
    def query(cls, sql, *, error=False, enabled=True, error_code=None):
        command = [ARGS.mysql, "-u", ARGS.user, "-N", "-B", "--raw", "--default-character-set=utf8mb4", "-Doceanbase"]
        if ARGS.socket:
            command += ["--protocol=SOCKET", "--socket", ARGS.socket]
        else:
            command += ["--protocol=TCP", "-h", ARGS.host, "-P", str(ARGS.port)]
        setup = "SET ob_query_timeout=20000000;"
        if enabled:
            setup += "SET SESSION enable_file_sql=ON;"
        result = subprocess.run(command, input=setup + sql + ";\n", text=True,
                                capture_output=True, timeout=40)
        cls.events.append({"sql": sql, "code": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
        if error:
            if result.returncode == 0 or not result.stderr.startswith("ERROR "):
                raise AssertionError(f"Expected SQL error: {sql}\n{result}")
            if any(f"ERROR {code}" in result.stderr for code in (2002, 2003, 2006, 2013)):
                raise AssertionError("Connection failure is not an expected SQL error: " + result.stderr)
        elif result.returncode:
            raise AssertionError(f"{sql}\n{result.stderr}")
        if error_code is not None and not result.stderr.startswith(f"ERROR {error_code} "):
            raise AssertionError(f"Expected SQL error {error_code}: {sql}\n{result.stderr}")
        return result.stdout.strip().splitlines()

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="seekdb-file-sql-test-")
        cls.root = Path(cls.directory.name)
        cls.query(f"SET GLOBAL secure_file_priv='{cls.root}'")
        source = Path(__file__).resolve().parents[4] / "tools/deploy/mysql_test/test_suite/file_sql/data"
        for name in ("basic.csv", "basic.jsonl", "invalid.txt"):
            (cls.root / name).write_bytes((source / name).read_bytes())
        (cls.root / "simple.parquet").write_bytes(base64.b64decode((source / "simple.parquet.b64").read_bytes()))
        for fixture in Path(__file__).with_name("data").glob("*.parquet"):
            (cls.root / fixture.name).write_bytes(fixture.read_bytes())

    @classmethod
    def tearDownClass(cls):
        if ARGS.sql_log:
            Path(ARGS.sql_log).write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in cls.events))
        cls.directory.cleanup()

    def setUp(self):
        self.test_directory = tempfile.TemporaryDirectory(dir=type(self).root)
        self.root = Path(self.test_directory.name)
        for source in type(self).root.iterdir():
            if source.is_file():
                (self.root / source.name).write_bytes(source.read_bytes())

    def tearDown(self):
        self.test_directory.cleanup()

    def file(self, name):
        return str(self.root / name)

    def test_disabled_and_options(self):
        self.assertEqual(["0"], self.query("SELECT @@session.enable_file_sql", enabled=False))
        self.query(f"SELECT * FROM read_csv('{self.file('basic.csv')}')", enabled=False, error=True, error_code=1235)
        for option in ("sample_size=10", "sample_size=NULL", "header='true'", "unknown=1", "header=true,header=false"):
            self.query(f"SELECT * FROM read_csv('{self.file('basic.csv')}',{option})", error=True)
        self.assertEqual(["3"], self.query(f"SELECT COUNT(*) FROM read_csv('{self.file('basic.csv')}',header=true,sample_size=-1)"))

    def test_csv_official(self):
        sql = f"FROM read_csv('{self.file('basic.csv')}') f"
        self.assertEqual(["1\tAlice\t98.5\t1\t2026-08-01\t2026-08-01 10:11:12\thello,<NL>world\t101",
                          "2\tBob\t87\t0\tNULL\tNULL\tplain\t102",
                          '3\tChen\t90.25\t1\t2026-08-03\t2026-08-03 12:13:14\tsay "hi"\t103'],
                         self.query("SELECT id,name,score,active,day,created,REPLACE(note,CHAR(10),'<NL>'),id__2 " + sql + " ORDER BY id"))
        self.assertEqual(["3\t6\t91.92\t2"], self.query("SELECT COUNT(*),SUM(id),ROUND(AVG(score),2),SUM(active) " + sql))

    def test_describe_and_glob(self):
        rows = self.query(f"DESCRIBE SELECT * FROM read_csv('{self.file('basic.csv')}')")
        self.assertEqual(["id\tBIGINT\tNO\tNULL\tNULL\tNULL", "name\tVARCHAR\tNO\tNULL\tNULL\tNULL",
                          "score\tDOUBLE\tNO\tNULL\tNULL\tNULL", "active\tBOOLEAN\tNO\tNULL\tNULL\tNULL",
                          "day\tDATE\tYES\tNULL\tNULL\tNULL", "created\tDATETIME\tYES\tNULL\tNULL\tNULL",
                          "note\tVARCHAR\tNO\tNULL\tNULL\tNULL", "id__2\tBIGINT\tNO\tNULL\tNULL\tNULL"], rows)
        self.assertEqual(["basic.csv"], self.query(f"SELECT SUBSTRING_INDEX(file,'/',-1) FROM glob('{self.root}/*.csv')"))
        self.assertEqual(["0"], self.query(f"SELECT COUNT(*) FROM glob('{self.root}/missing-*.csv')"))

    def test_jsonl(self):
        self.assertEqual(["1\tAlice\t98.5\t1\tNULL\tNULL", "2\tBob\tNULL\t0\t2026-08-02\tNULL", "3\tChen\t90\t1\tNULL\tok"],
                         self.query(f"SELECT id,name,score,active,day,note FROM read_json('{self.file('basic.jsonl')}',format='newline_delimited') j ORDER BY id"))
        for name, text in (("nested", '{"a":{"b":1}}'), ("array", '{"a":[1]}'),
                           ("bad", '{"a":'), ("duplicate", '{"a":1,"a":2}')):
            path = self.root / (name + ".jsonl")
            path.write_text(text + "\n")
            self.query(f"SELECT * FROM read_ndjson('{path}')", error=True)

    def test_types_and_all_rows_inferred(self):
        path = self.root / "types.csv"
        path.write_text("n,b,d,t,nil\n9223372036854775807,true,2024-02-29,2024-02-29T23:59:59,\n"
                        "-9223372036854775808,false,2023-02-29,2024-02-29 00:00:00,\n")
        rows = self.query(f"DESCRIBE SELECT * FROM read_csv('{path}')")
        self.assertEqual(["BIGINT", "BOOLEAN", "VARCHAR", "DATETIME", "NULL"], [r.split("\t")[1] for r in rows])
        self.assertEqual(["2\t0"], self.query(f"SELECT COUNT(*),COUNT(nil) FROM read_csv('{path}')"))
        path.write_text("v\n" + "1\n" * 2100 + "late string\n")
        self.assertEqual(["VARCHAR"], [r.split("\t")[1] for r in self.query(f"DESCRIBE SELECT * FROM read_csv('{path}')")])

    def test_csv_options_and_errors(self):
        path = self.root / "options.csv"
        path.write_bytes(b'\xef\xbb\xbfa;b\r\n1;""\r\n2;NULL\r\n3;"x;\r\ny"\r\n')
        self.assertEqual(["1\t0\t0", "2\t1\tNULL", "3\t0\t5"],
                         self.query(f"SELECT a,b IS NULL,LENGTH(b) FROM read_csv('{path}',delim=';',nullstr='NULL') ORDER BY a"))
        for content in ('a,b\n1\n', 'a\n"unterminated', 'a\n"quoted"junk\n'):
            path.write_text(content)
            self.query(f"SELECT * FROM read_csv('{path}')", error=True)

    def test_native_sql_and_joins(self):
        csv_source = f"read_csv('{self.file('basic.csv')}')"
        json_source = f"read_json('{self.file('basic.jsonl')}')"
        self.assertEqual(["2\tBOB\t174"], self.query(f"SELECT id,UPPER(name),score*2 FROM {csv_source} f WHERE id>1 ORDER BY id LIMIT 1"))
        self.assertEqual(["0\t1\t87", "1\t2\t188.75"], self.query(f"SELECT active,COUNT(*),SUM(score) FROM {csv_source} GROUP BY active HAVING COUNT(*)>0 ORDER BY active"))
        self.assertEqual(["1\tAlice", "2\tBob", "3\tChen"], self.query(f"SELECT a.id,b.name FROM {csv_source} a JOIN {json_source} b ON a.id=b.id ORDER BY a.id"))
        self.assertEqual(["1\tNULL", "2\tNULL", "3\tNULL"], self.query(f"SELECT a.id,b.name FROM {csv_source} a LEFT JOIN {json_source} b ON a.id=b.id+100 ORDER BY a.id"))
        self.assertEqual(["9"], self.query(f"SELECT COUNT(*) FROM {csv_source} a CROSS JOIN {csv_source} b"))
        self.assertTrue(self.query(f"EXPLAIN SELECT id FROM {csv_source} WHERE id>1"))

    def test_parquet(self):
        self.assertEqual(["7\t0\t6\t21"], self.query(f"SELECT COUNT(*),MIN(c1),MAX(c1),SUM(c1) FROM read_parquet('{self.file('simple.parquet')}') p"))
        for optional in (False, True):
            for compressed in (False, True):
                for dictionary in (False, True):
                    rows = [1, None, 3, 4, None] if optional else [1, 2, 3, 4, 5]
                    path = self.root / "generated.parquet"
                    parquet(path, rows, nullable=optional, snappy=compressed, dictionary=dictionary)
                    self.assertEqual(["5\t8\t3" if optional else "5\t15\t5"],
                                     self.query(f"SELECT COUNT(*),SUM(n),COUNT(n) FROM read_parquet('{path}')"))
        path.write_bytes(b"PAR1bad")
        self.query(f"SELECT * FROM read_parquet('{path}')", error=True)

    def test_copy_round_trip_and_refuse_overwrite(self):
        path = self.root / "export.csv"
        self.query(f"COPY (SELECT id,name,note FROM read_csv('{self.file('basic.csv')}') ORDER BY id) TO '{path}' (FORMAT csv,HEADER,NULLSTR 'NULL')")
        self.assertEqual(["1\tAlice", "2\tBob", "3\tChen"], self.query(f"SELECT id,name FROM read_csv('{path}') ORDER BY id"))
        with path.open(newline="") as stream:
            rows = list(csv.reader(stream))
        self.assertEqual(["id", "name", "note"], rows[0])
        self.assertEqual("hello,\nworld", rows[1][2])
        self.assertEqual('say "hi"', rows[3][2])
        before = path.read_bytes()
        self.query(f"COPY (SELECT 99) TO '{path}'", error=True)
        self.assertEqual(before, path.read_bytes())
        target = self.root / "failed.csv"
        bad_source = self.root / "copy-query-error.csv"
        bad_source.write_text('s\n"{}"\nbad\n')
        self.query(f"COPY (SELECT CAST(s AS JSON) FROM read_csv('{bad_source}')) TO '{target}'", error=True)
        self.assertFalse(target.exists())
        self.assertEqual([], list(self.root.glob(".seekdb-copy-*.tmp")))

    def test_ordinary_sql_and_copy_table(self):
        self.query("DROP TABLE IF EXISTS file_sql_regression")
        try:
            self.query("CREATE TABLE file_sql_regression(id BIGINT PRIMARY KEY, s VARCHAR(30), "
                       "twice BIGINT GENERATED ALWAYS AS (id*2) STORED)")
            self.query("INSERT INTO file_sql_regression(id,s) VALUES (1,'one'),(2,NULL)")
            self.assertEqual(["2\t6\t1"], self.query(
                "SELECT COUNT(*),SUM(twice),COUNT(s) FROM file_sql_regression"))
            self.assertEqual(["1", "2"], self.query(
                "SELECT v FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(v INT PATH '$')) jt ORDER BY v"))
            path = self.root / "copy-table.csv"
            self.query(f"COPY file_sql_regression TO '{path}' (HEADER,DELIMITER ';',NULLSTR 'NULL')")
            self.assertEqual(["1\tone\t2", "2\tNULL\t4"], self.query(
                f"SELECT * FROM read_csv('{path}',delimiter=';',nullstr='NULL') ORDER BY id"))
        finally:
            self.query("DROP TABLE IF EXISTS file_sql_regression")

    def test_parquet_independent_writer_and_types(self):
        fixtures = sorted(self.root.glob("arrow_*.parquet"))
        self.assertEqual(4, len(fixtures))
        for path in fixtures:
            source = f"read_parquet('{path}')"
            self.assertEqual(["4\t2\t2\t9\t0.75\t-0.5"], self.query(
                f"SELECT COUNT(*),SUM(b),SUM(i32),SUM(i64),SUM(f32),SUM(f64) FROM {source}"))
            self.assertEqual(["E4BDA0E5A5BD\t2024-02-29\t2024-02-29 23:59:59.123456"], self.query(
                f"SELECT HEX(text),day,created FROM {source} WHERE i32=1"))
            self.assertEqual(["3\t1970-01-01\t2026-10-27"], self.query(
                f"SELECT COUNT(text),MIN(day),MAX(day) FROM {source}"))

    def test_security(self):
        self.query("SELECT * FROM read_csv('/etc/passwd')", error=True, error_code=1227)
        self.query(f"SELECT * FROM read_csv('{self.root}/../etc/passwd')", error=True)
        link = self.root / "escape.csv"
        link.symlink_to("/etc/passwd")
        self.query(f"SELECT * FROM read_csv('{link}')", error=True, error_code=1227)
        self.query(f"SELECT * FROM glob('{self.root}/escape.*')", error=True, error_code=1227)
        self.query("COPY (SELECT 1) TO '/tmp/seekdb-file-sql-escape.csv'", error=True, error_code=1227)
        self.query(f"SELECT * FROM read_csv('{self.root}')", error=True)

    def test_large_streaming_scan(self):
        path = self.root / "large.csv"
        with path.open("w") as stream:
            stream.write("id,text\n")
            for i in range(100000):
                stream.write(f"{i},记录-{i}-" + "x" * 80 + "\n")
        self.assertEqual(["100000\t4999950000"],
                         self.query(f"SELECT COUNT(*),SUM(id) FROM read_csv('{path}')"))
        self.assertEqual(["99999"], self.query(f"SELECT id FROM read_csv('{path}') ORDER BY id DESC LIMIT 1"))

    def test_copy_options_lob_and_no_options(self):
        path = self.root / "no-options.csv"
        self.query(f"COPY (SELECT 7 AS n) TO '{path}'")
        self.assertEqual(["7"], self.query(f"SELECT n FROM read_csv('{path}')"))
        path = self.root / "escapes.csv"
        source = self.root / "escapes-source.csv"
        source.write_text("n,s\n1,\"a\\b;it's\"\n2,\n3,\"\"\n")
        self.query(f"COPY (SELECT * FROM read_csv('{source}')) TO '{path}' "
                   "(HEADER,DELIMITER ';',QUOTE '''',ESCAPE '~',NULLSTR 'NULL')")
        self.assertEqual(["1\ta\\b;it's", "2\tNULL", "3\t0"], self.query(
            f"SELECT n,IF(s='',LENGTH(s),s) FROM read_csv('{path}',delim=';',quote='''',escape='~',nullstr='NULL') ORDER BY n"))
        self.query("DROP TABLE IF EXISTS file_sql_lob_regression")
        try:
            self.query("CREATE TABLE file_sql_lob_regression(id INT PRIMARY KEY,s LONGTEXT,j JSON)")
            self.query("INSERT INTO file_sql_lob_regression VALUES (1,REPEAT('中',20000),JSON_OBJECT('x',7))")
            path = self.root / "lob.csv"
            self.query(f"COPY file_sql_lob_regression TO '{path}' (HEADER)")
            self.assertEqual(["60000\t7"], self.query(
                f"SELECT LENGTH(s),JSON_EXTRACT(j,'$.x') FROM read_csv('{path}')"))
        finally:
            self.query("DROP TABLE IF EXISTS file_sql_lob_regression")

    def test_glob_directories_and_limit(self):
        nested = self.root / "nested"
        nested.mkdir()
        (nested / "one.csv").write_text("n\n1\n")
        self.assertEqual(["one.csv"], self.query(
            f"SELECT SUBSTRING_INDEX(file,'/',-1) FROM glob('{self.root}/nest*/*.csv')"))
        for i in range(10001):
            (nested / f"limit-{i}").touch()
        self.query(f"SELECT COUNT(*) FROM glob('{nested}/limit-*')", error=True, error_code=4019)
        self.assertEqual(["1"], self.query("SELECT 1"))

    def test_file_changes_during_scan(self):
        path = self.root / "changing.csv"
        path.write_text("id\n" + "1\n" * 1000)
        # Inference finishes before the SQL SLEEP expressions make scanning slow.
        timer = threading.Timer(2, lambda: path.write_text("id\n2\n"))
        timer.start()
        try:
            self.query(f"SELECT SLEEP(0.01),id FROM read_csv('{path}')", error=True)
            self.assertIn("input changed during scan", self.events[-1]["stderr"])
        finally:
            timer.join()
        self.assertEqual(["1"], self.query("SELECT 1"))

    def test_file_changes_between_statements(self):
        path = self.root / "replace.csv"
        path.write_text("n\n1\n")
        self.assertEqual(["1"], self.query(f"SELECT n FROM read_csv('{path}')"))
        path.write_text("different\nupdated\n")
        self.assertEqual(["updated"], self.query(f"SELECT different FROM read_csv('{path}')"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=12892)
    parser.add_argument("--socket")
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--user", default="root")
    parser.add_argument("--sql-log")
    ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining], verbosity=2)
