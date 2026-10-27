# File SQL integration coverage

Build the server with `./build.py build //src/observer:seekdb -j2` and start a
separate local instance. Run the integration test against that instance:

```bash
python3 unittest/sql/engine/basic/test_file_sql_integration.py \
  --host INSTANCE_ADDRESS --port 12892 --sql-log /tmp/file-sql-results.jsonl
```

Alternatively pass `--socket PATH`. Tests set `secure_file_priv` on the selected
instance to a temporary fixture directory, so use a disposable instance. They
run SQL through the MySQL client and compare returned values, column metadata,
CSV bytes and errors. Connection failures fail the test; they cannot satisfy an
expected SQL error and are not retried.

The 17 test groups cover the official CSV, JSONL and Parquet examples, alternate CSV
options and malformed rows, JSONL field unions and missing values, full-file
type inference, SQL expressions/aggregation/ordering, INNER/LEFT/cross-format
joins, schema changes between statements, secure paths, COPY round trips and
cleanup on failure, large TEXT/JSON values, escaped output, glob directory patterns
and resource limits. Additional Parquet fixtures exercise multiple row groups,
required/nullable columns, PLAIN/dictionary encoding, and Snappy compression.
The fixture writer implements only the data file format; seekdb executes all
queries. No external query engine or model service is used.

The original competition `file_sql_basic.test`, expected result and data files
are kept under `tools/deploy/mysql_test/test_suite/file_sql/`. Its deployment
includes require the competition mysqltest environment and SSH configuration.
These tests do not replace hidden competition tests or sustained resource and
concurrency testing.

Validation on 2026-10-27 used the actual observer binary and an isolated local
instance: all 17 groups passed. The official file_sql_basic mysqltest passed
against its unchanged expected result; only fixture deployment includes and the
local directory were adapted in a temporary test copy. The three existing AI
integration suites also passed (16 completion/embedding, 14 rerank, 13 embedding
batch groups). See `docs/competition/file_sql_validation.md` for scope and limits.
