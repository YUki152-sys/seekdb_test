/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{
class FileCopyWriter
{
public:
  FileCopyWriter();
  ~FileCopyWriter();
  int open(const FileScanDef &def, ObSQLSessionInfo &session);
  int write_row(const common::ObIArray<common::ObObj> &values);
  int finish();
  void abort();
private:
  int write_field(const common::ObString &text, bool is_null);
  int write_bytes(const char *data, int64_t length);
  const FileScanDef *def_;
  int directory_fd_;
  FILE *file_;
  char target_[256];
  char temporary_[256];
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(FileCopyWriter);
};
} // namespace sql
} // namespace oceanbase
