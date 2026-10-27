/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
struct FileScanDef;
struct FileCell;
struct ParquetValue;
struct ParquetColumnCursor;

// A bounded footer plus one decoded page per projected column. No query engine
// or table materialization is involved in decoding the file format.
class FileParquetReader
{
public:
  FileParquetReader();
  ~FileParquetReader();
  static int infer(int fd, FileScanDef &def, common::ObIAllocator &allocator);
  int open(int fd, const FileScanDef &def);
  int project(const common::ObIArray<int64_t> &columns);
  int next(common::ObIArray<FileCell> &cells, common::ObIAllocator &row_allocator);
  int rescan();
  void close();
private:
  int open_group();
  int load_page(ParquetColumnCursor &cursor);
  int decode_page(ParquetColumnCursor &cursor, const ParquetValue &header, char *decoded, int64_t size);
  int next_cell(ParquetColumnCursor &cursor, FileCell &cell, common::ObIAllocator &allocator);
  int fd_;
  const FileScanDef *def_;
  common::ObArenaAllocator metadata_;
  common::ObArenaAllocator group_memory_;
  ParquetValue *footer_;
  common::ObSEArray<int64_t, 16> projection_;
  common::ObSEArray<ParquetColumnCursor *, 16> cursors_;
  int64_t group_index_;
  int64_t row_in_group_;
  int64_t group_rows_;
  int64_t allocated_page_bytes_;
  DISALLOW_COPY_AND_ASSIGN(FileParquetReader);
};
} // namespace sql
} // namespace oceanbase
