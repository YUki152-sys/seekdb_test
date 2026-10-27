/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_copy_writer.h"
#include "sql/session/ob_sql_session_info.h"
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <sys/random.h>

namespace oceanbase
{
using namespace common;
namespace sql
{
FileCopyWriter::FileCopyWriter()
    : def_(nullptr), directory_fd_(-1), file_(nullptr), target_(), temporary_(), allocator_("FileCopy")
{}
FileCopyWriter::~FileCopyWriter() { abort(); }

int FileCopyWriter::open(const FileScanDef &def, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  abort(); def_ = &def;
  const ObString &path = def.path_;
  char input[PATH_MAX];
  if (path.empty() || path.length() >= PATH_MAX || memchr(path.ptr(), '\0', path.length()) != nullptr) { ret = OB_INVALID_ARGUMENT; }
  else {
    MEMCPY(input, path.ptr(), path.length()); input[path.length()] = '\0';
    char *slash = strrchr(input, '/');
    const char *name = slash == nullptr ? input : slash + 1;
    int64_t length = strlen(name);
    if (length == 0 || length >= sizeof(target_) || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) { ret = OB_INVALID_ARGUMENT; }
    else {
      MEMCPY(target_, name, length + 1);
      if (slash == nullptr) { strcpy(input, "."); }
      else if (slash == input) { slash[1] = '\0'; }
      else { *slash = '\0'; }
      ObString canonical;
      OZ(FileScanReader::secure_open(ObString(input), session, allocator_, canonical, directory_fd_, true));
    }
  }
  struct stat existing;
  if (OB_SUCC(ret) && (::fstatat(directory_fd_, target_, &existing, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)) {
    ret = OB_FILE_ALREADY_EXIST;
  }
  uint64_t random = 0;
  if (OB_SUCC(ret) && ::getrandom(&random, sizeof(random), 0) != sizeof(random)) { ret = OB_IO_ERROR; }
  if (OB_SUCC(ret)) {
    snprintf(temporary_, sizeof(temporary_), ".seekdb-copy-%lx.tmp", random);
    int fd = ::openat(directory_fd_, temporary_, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) { temporary_[0] = '\0'; ret = OB_IO_ERROR; }
    else if ((file_ = ::fdopen(fd, "wb")) == nullptr) { ::close(fd); ret = OB_IO_ERROR; }
  }
  if (OB_SUCC(ret) && def.header_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < def.columns_.count(); ++i) {
      if (i > 0) { char delimiter = def.delimiter_; OZ(write_bytes(&delimiter, 1)); }
      OZ(write_field(def.columns_.at(i).name_, false));
    }
    OZ(write_bytes("\n", 1));
  }
  if (OB_FAIL(ret)) { abort(); }
  return ret;
}

int FileCopyWriter::write_bytes(const char *data, int64_t length)
{
  int ret = OB_SUCCESS;
  if (file_ == nullptr || length < 0) { ret = OB_NOT_INIT; }
  else if (length > 0 && ::fwrite(data, 1, length, file_) != static_cast<size_t>(length)) { ret = OB_IO_ERROR; }
  return ret;
}

int FileCopyWriter::write_field(const ObString &text, bool is_null)
{
  int ret = OB_SUCCESS;
  if (is_null) { OZ(write_bytes(def_->nullstr_.ptr(), def_->nullstr_.length())); }
  else {
    bool quote = text.empty() || text == def_->nullstr_;
    for (int64_t i = 0; !quote && i < text.length(); ++i) {
      quote = text[i] == def_->delimiter_ || text[i] == def_->quote_ || text[i] == '\n' || text[i] == '\r';
    }
    char quotation = def_->quote_, escape = def_->escape_;
    if (quote) { OZ(write_bytes(&quotation, 1)); }
    for (int64_t i = 0; OB_SUCC(ret) && i < text.length(); ++i) {
      if (quote && (text[i] == quotation || (escape != quotation && text[i] == escape))) { OZ(write_bytes(&escape, 1)); }
      OZ(write_bytes(text.ptr() + i, 1));
    }
    if (quote) { OZ(write_bytes(&quotation, 1)); }
  }
  return ret;
}

int FileCopyWriter::write_row(const ObIArray<ObObj> &values)
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  char *buffer = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
    if (i > 0) { char delimiter = def_->delimiter_; OZ(write_bytes(&delimiter, 1)); }
    const ObObj &value = values.at(i);
    ObString text;
    if (value.is_string_type()) { text = value.get_string(); }
    else if (!value.is_null()) {
      if (buffer == nullptr) { buffer = static_cast<char *>(allocator_.alloc(1L << 20)); }
      if (buffer == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      else {
        int64_t pos = 0; OZ(value.print_plain_str_literal(buffer, 1L << 20, pos));
        text.assign_ptr(buffer, pos);
      }
    }
    OZ(write_field(text, value.is_null()));
  }
  OZ(write_bytes("\n", 1));
  return ret;
}

int FileCopyWriter::finish()
{
  int ret = OB_SUCCESS;
  if (file_ == nullptr) { ret = OB_NOT_INIT; }
  else if (::fflush(file_) != 0 || ::fsync(::fileno(file_)) != 0) { ret = OB_IO_ERROR; }
  if (file_ != nullptr) { if (::fclose(file_) != 0) { ret = OB_IO_ERROR; } file_ = nullptr; }
  if (OB_SUCC(ret) && ::linkat(directory_fd_, temporary_, directory_fd_, target_, 0) != 0) {
    ret = errno == EEXIST ? OB_FILE_ALREADY_EXIST : OB_IO_ERROR;
  }
  abort();
  return ret;
}

void FileCopyWriter::abort()
{
  if (file_ != nullptr) { ::fclose(file_); file_ = nullptr; }
  if (directory_fd_ >= 0) {
    if (temporary_[0] != '\0') { ::unlinkat(directory_fd_, temporary_, 0); }
    ::close(directory_fd_); directory_fd_ = -1;
  }
  target_[0] = '\0'; temporary_[0] = '\0'; allocator_.reset(); def_ = nullptr;
}
} // namespace sql
} // namespace oceanbase
