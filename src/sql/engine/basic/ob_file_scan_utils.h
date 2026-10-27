/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once

#include <stdio.h>
#include <sys/stat.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "lib/utility/ob_unify_serialize.h"

typedef struct _ParseNode ParseNode;
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class FileParquetReader;

struct FileColumn
{
  OB_UNIS_VERSION(1);
public:
  enum Type { NIL, INTEGER, REAL, BOOLEAN, TEXT, DATE, DATETIME };
  FileColumn() : name_(), type_(NIL), nullable_(false), max_length_(0), scale_(0) {}
  common::ObString name_;
  int64_t type_;
  bool nullable_;
  int64_t max_length_;
  int64_t scale_;
};

// Only the schema and file identity enter the plan. Rows remain in the file.
struct FileScanDef
{
  OB_UNIS_VERSION(1);
public:
  enum Format { CSV, JSONL, PARQUET, GLOB };
  FileScanDef();
  int parse(const ParseNode &node, ObSQLSessionInfo &session, common::ObIAllocator &alloc);
  int infer(ObSQLSessionInfo &session, common::ObIAllocator &alloc);
  int encode(common::ObIAllocator &alloc, common::ObString &value) const;
  int decode(const common::ObString &value);
  int add_column(const common::ObString &name, common::ObIAllocator &alloc);
  int64_t find_column(const common::ObString &name) const;
  bool describe_;
  common::ObSEArray<FileColumn, 16> described_columns_;
  common::ObSEArray<common::ObString, 16> discovered_paths_;
  int64_t format_;
  common::ObString path_;
  common::ObString nullstr_;
  bool header_;
  int64_t delimiter_;
  int64_t quote_;
  int64_t escape_;
  int64_t device_;
  int64_t inode_;
  int64_t size_;
  int64_t mtime_sec_;
  int64_t mtime_nsec_;
  int64_t ctime_sec_;
  int64_t ctime_nsec_;
  common::ObSEArray<FileColumn, 16> columns_;
};

struct FileCell
{
  FileCell() : text_(), null_(false), quoted_(false), typed_(false), value_() {}
  common::ObString text_;
  bool null_;
  bool quoted_;
  bool typed_;
  common::ObObj value_;
};

struct FileRecord
{
  common::ObSEArray<FileCell, 16> cells_;
  common::ObSEArray<common::ObString, 16> names_;
};

class FileScanReader
{
public:
  FileScanReader();
  ~FileScanReader();
  int open(const FileScanDef &def, ObSQLSessionInfo &session, bool infer = false);
  int next(common::ObIArray<FileCell> &cells, common::ObIArray<common::ObString> &names);
  int next_batch(const common::ObIArray<FileRecord *> *&records);
  int project(const common::ObIArray<int64_t> &columns);
  int rescan();
  int check_identity() const;
  void close();
  int64_t row_number() const { return row_number_; }
  common::ObIAllocator &row_allocator() { return row_alloc_; }
  static int check_enabled(ObSQLSessionInfo &session);
  static int secure_open(const common::ObString &path, ObSQLSessionInfo &session,
                         common::ObIAllocator &alloc, common::ObString &canonical, int &fd, bool directory = false);
private:
  int next_record(common::ObIArray<FileCell> &cells, common::ObIArray<common::ObString> &names);
  void clear_batch();
  int next_csv(common::ObIArray<FileCell> &cells);
  int next_json(common::ObIArray<FileCell> &cells, common::ObIArray<common::ObString> &names);
  int read_line(common::ObString &line);
  int next_metadata(common::ObIArray<FileCell> &cells);
  const FileScanDef *def_;
  FILE *file_;
  FileParquetReader *parquet_;
  struct stat identity_;
  int64_t row_number_;
  common::ObArenaAllocator row_alloc_;
  common::ObSEArray<FileRecord *, 64> batch_;
  DISALLOW_COPY_AND_ASSIGN(FileScanReader);
};

class FileScanUtils
{
public:
  static int64_t infer_type(const common::ObString &text);
  static int64_t widen(int64_t left, int64_t right);
  static const char *type_name(int64_t type);
  static common::ObObjType obj_type(int64_t type);
  static int convert(const FileCell &cell, const FileColumn &column, common::ObObj &value);
};
} // namespace sql
} // namespace oceanbase
