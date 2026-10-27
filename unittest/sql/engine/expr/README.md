# AI function integration regression

`test_ai_functions_integration.py` runs SQL through the MySQL protocol and
captures the HTTP requests made by seekdb. Its local Mock implements the
OpenAI and Ollama completion and embedding response shapes. Python's standard
library and a `mysql` client are sufficient; model credentials are unnecessary.

Build and start an isolated local instance in a separate terminal:

```bash
./build.py build //src/observer:seekdb -j3
TASK_INSTANCE=$(mktemp -d /tmp/seekdb-ai-test.XXXXXX)
build/bin/src/observer/seekdb --base-dir="$TASK_INSTANCE" --port=12891 \
  --nodaemon --log-level=INFO \
  --parameter memory_budget=2G --parameter cpu_count=2 \
  --parameter datafile_size=1G --parameter log_disk_size=2G
```

After the instance accepts SQL connections, run:

```bash
python3 unittest/sql/engine/expr/test_ai_functions_integration.py \
  --port 12891 --requests-log /tmp/seekdb-ai-requests.jsonl \
  --sql-log /tmp/seekdb-ai-sql.jsonl
```

The script asserts SQL values and HTTP request bodies/headers. Coverage includes
Endpoint CRUD and Provider changes; missing/empty keys; case-insensitive Ollama;
non-streaming generation; model-name overrides; textual and native JSON config;
TEXT/LONGTEXT, Unicode and nested/repeated Prompt arguments; native and explicit
embedding dimensions, including table rows; malformed responses; and recovery
after HTTP or connection errors. After each bad-response case it checks a normal
AI call and ordinary SQL. Test objects have unique names and are removed by
class cleanup; HTTP request logs contain synthetic fixture credentials only.

The fixture's native embedding dimension is deliberately seven. Tests require
an omitted SQL dimension to preserve the model's native size, and an explicit
dimension to appear in the request and match the returned vector.

Stop the isolated server with Ctrl-C in its terminal after testing. This suite
does not assess real model quality, AI_RERANK, or batching inputs across rows.

`test_ai_rerank_integration.py` uses the same SQL client and cleanup helpers,
with a separate Mock for SiliconFlow/Cohere and DashScope rerank protocols:

```bash
python3 unittest/sql/engine/expr/test_ai_rerank_integration.py \
  --port 12891 --requests-log /tmp/seekdb-rerank-requests.jsonl \
  --sql-log /tmp/seekdb-rerank-sql.jsonl
```

It checks string and object arrays, textual/native JSON and table inputs,
custom text fields, original IDs and metadata, batch boundaries, global index
mapping, descending scores and original input order for ties. Requests must
contain at most 20 documents and request every result in each batch. Responses
are deliberately shuffled; missing/extra/duplicate/out-of-range indices,
noninteger indices, invalid scores, malformed response shapes, HTTP errors and
a failed second batch must produce SQL errors. Each bad-response case is
followed by ordinary SQL and a successful rerank. MySQL connection failures
cannot satisfy an expected SQL error; the suite does not retry failed calls.

`test_ai_embed_batch_integration.py` verifies SQL engine batching across table
rows with OpenAI, DashScope and Ollama Mock protocols:

```bash
python3 unittest/sql/engine/expr/test_ai_embed_batch_integration.py \
  --port 12891 --requests-log /tmp/seekdb-embed-batch-requests.jsonl \
  --sql-log /tmp/seekdb-embed-batch-sql.jsonl
```

Each vector encodes its source row, so reordered or duplicated results fail
value assertions. OpenAI `index` and DashScope `text_index` responses are
shuffled; Ollama responses use its positional protocol. Coverage includes
LONGTEXT/Unicode, native and row-dependent dimensions, mixed models, NULL
content/model propagation with no HTTP input for those rows, filters, LIMIT
and OFFSET, short-circuit CASE, repeated expressions, HTTP batch boundaries,
1,150 rows crossing executor batches, invalid indices/vectors, failed second
batches and network recovery. Compatible consecutive rows share a request of
at most 20 inputs; executor batches may be smaller, and changing model or
dimension starts another request. Scalar SQL remains covered.

Run the suites sequentially to avoid concurrent model/Endpoint DDL conflicts.
Use `--host INSTANCE_ADDRESS` to choose an instance's TCP address. Failed
connections are reported as failures without automatic retries, including in
tests expecting SQL errors. These local tests do not replace the competition
Mock or official submission checks.
