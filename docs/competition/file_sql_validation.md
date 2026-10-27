# 第二大题：文件即数据库验证记录

日期：2026-10-27。分支 `feat/competition-file-sql`，基于第一题提交
`e42e86f7e019111508779d5fe21414f9e18c0e9b`。

## 实现

文件入口使用 seekdb 原生 parser、resolver、FUNCTION_TABLE 算子和 SELECT INTO
算子；过滤、排序、聚合、JOIN 使用 seekdb 执行计划。没有外部 SQL 引擎。
CSV/JSONL 全量推断后重新打开流式扫描，批次上限 64 行，当前批内存达到
1 MiB 后停止取数（允许一条最大记录）。Parquet 只读取投影列，支持多 Row
Group、可空基础类型、PLAIN、dictionary、RLE、DataPage V1/V2 和 Snappy。
COPY 使用受限目录 FD 和独占临时文件，无覆盖发布，失败清理临时文件。

默认关闭 `enable_file_sql`。启用后仍要求有效、非空的 `secure_file_priv`；
路径先规范化并校验范围，再通过逐层 `openat/O_NOFOLLOW` 打开。推断、执行
和每行读取检查设备号、inode、大小及纳秒级 mtime/ctime。

## 实测发现及修复

- 文件函数实参被重复封装、常量 flags 尚未生成，导致解析或 resolver 误分派；
  修正实参结构并 formalize 常量，按 raw expression 类型分派。
- VARCHAR 比较级别缺失导致表达式和直接投影报内部错误；补齐隐式比较级别。
- Parquet 到达末尾时保留旧 Row Group 行数，导致重复取数和超时；清零组状态。
- 使用最大数值精度导致 DOUBLE 固定输出 30 位小数；改用默认精度，DATETIME
  按实际小数秒精度输出。COPY 路径与文件参数禁止参数化，保留解析期常量。
- glob 事后限制结果数量仍可能提前大量分配；改为有界目录遍历。
- COPY 大文本及 JSON 需要解引用 LOB；增加有界读取和 JSON 文本转换。

## 验证结果

构建命令：`./build.py build //src/observer:seekdb -j3`，成功。
测试使用隔离实例端口 12892 和 Unix socket，未改变项目部署实例。

| 验证 | 结果 |
| --- | --- |
| 自建真实 SQL 集成测试 | 17/17 通过 |
| 官方 file_sql_basic mysqltest | 与官方原始 expected result 完全匹配 |
| 第一题 completion/embedding 回归 | 16/16 通过 |
| 第一题 rerank 回归 | 14/14 通过 |
| 第一题 embedding batch 回归 | 13/13 通过 |
| `git diff --check` | 通过 |

自建测试逐值断言官方 CSV/JSONL 示例、元数据、Parquet 聚合、4 个独立 Arrow
生成文件及 8 种自建编码组合、跨格式 INNER/LEFT JOIN、重扫、原生执行计划、
COPY 读回和失败清理、普通表/生成列/JSON_TABLE、10 万行扫描、文件替换、
扫描中修改、路径权限错误码及 glob 数量上限。运行测试不依赖 pyarrow；
pyarrow 只用于离线生成输入 fixture。

官方 mysqltest 使用临时副本，仅替换 SSH fixture 部署/清理 includes 和文件
目录；SQL、错误码断言及 expected result 保留。`secure_file_priv` 在连接
建立前配置，避免旧 session 值影响测试。

最终测试二进制 SHA-256：
`5a3c10fe2d64b8ee29e3602ca1a6594bc14fd2e82fd5e77476a61efb9336a429`。
本机原始证据位于 `/tmp/seekdb-file-sql-evidence/`，包括构建日志、SQL 输入和
输出、HTTP Mock 请求记录、官方 mysqltest 临时副本及结果。

## 范围与限制

此验证不替代平台隐藏测试、长时间压力测试或恶意文件 fuzzing。当前 Parquet
实现支持无压缩及 Snappy；其他压缩格式、嵌套类型及不支持的编码返回错误。
记录上限 8 MiB，列上限 1024；Parquet Footer 上限 16 MiB、单页上限 32 MiB、
扫描页/字典预算 128 MiB；glob 最多访问 100000 个目录项、返回 10000 个文件，
结果路径累计最多 8 MiB。CSV/JSONL 只支持 `sample_size=-1`。

新增 C++ 头文件使用 `#pragma once`，容器使用项目 ObSEArray，测试位于既有
unittest 目录；tools 中只保留从官方比赛分支原样引入的测试。
