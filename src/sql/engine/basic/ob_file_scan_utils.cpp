/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <fnmatch.h>
#include "common/json_type/ob_json_parse.h"
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include "sql/parser/parse_node.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/string/ob_string_buffer.h"
#include "common/json_type/ob_json_tree.h"
#include "common/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
const int64_t MAX_FILE_ROW_BYTES = 8L * 1024 * 1024;
const int64_t MAX_FILE_COLUMNS = 1024;
ObString node_text(const ParseNode &node)
{
  return ObString(static_cast<int32_t>(node.str_len_), node.str_value_);
}
bool same_identity(const struct stat &left, const struct stat &right)
{
  return left.st_dev == right.st_dev && left.st_ino == right.st_ino
      && left.st_size == right.st_size
      && left.st_mtim.tv_sec == right.st_mtim.tv_sec
      && left.st_mtim.tv_nsec == right.st_mtim.tv_nsec
      && left.st_ctim.tv_sec == right.st_ctim.tv_sec
      && left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}
int file_error(int code, const char *message)
{
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, message);
  return code;
}
void record_error(const FileScanDef &def, int64_t row, const char *reason)
{
  char message[1024];
  snprintf(message, sizeof(message), "file SQL %.*s, record %ld: %s",
           MIN(512, def.path_.length()), def.path_.ptr(), row, reason);
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, message);
}
int check_file_scope(ObSQLSessionInfo &session, const ObString &canonical)
{
  int ret = OB_SUCCESS;
  ObString allowed;
  char configured[PATH_MAX];
  char resolved[PATH_MAX];
  OZ(session.get_secure_file_priv(allowed));
  if (OB_SUCC(ret) && (allowed.empty() || allowed.length() >= PATH_MAX
      || memchr(allowed.ptr(), '\0', allowed.length()) != nullptr)) { ret = OB_ERR_NO_PRIVILEGE; }
  if (OB_SUCC(ret)) {
    MEMCPY(configured, allowed.ptr(), allowed.length()); configured[allowed.length()] = '\0';
    if (::realpath(configured, resolved) == nullptr) { ret = OB_ERR_NO_PRIVILEGE; }
    else {
      ObString root(resolved);
      if (!canonical.prefix_match(root) || (root != "/" && canonical.length() > root.length()
          && canonical[root.length()] != '/')) { ret = OB_ERR_NO_PRIVILEGE; }
    }
    OZ(ObResolverUtils::check_secure_path(allowed, canonical));
  }
  if (OB_FAIL(ret)) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file SQL path is outside secure_file_priv"); }
  return ret;
}
bool valid_iso_date(const ObString &text)
{
  bool valid = text.length() == 10 && text[4] == '-' && text[7] == '-';
  for (int64_t i = 0; valid && i < text.length(); ++i) {
    if (i != 4 && i != 7 && (text[i] < '0' || text[i] > '9')) { valid = false; }
  }
  if (valid) {
    int year = (text[0]-'0')*1000+(text[1]-'0')*100+(text[2]-'0')*10+text[3]-'0';
    int month = (text[5]-'0')*10+text[6]-'0';
    int day = (text[8]-'0')*10+text[9]-'0';
    const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    valid = year > 0 && month >= 1 && month <= 12 && day >= 1
        && day <= days[month] + (month == 2 && leap ? 1 : 0);
  }
  return valid;
}
}

OB_SERIALIZE_MEMBER(FileColumn, name_, type_, nullable_, max_length_, scale_);
OB_SERIALIZE_MEMBER(FileScanDef, describe_, described_columns_, discovered_paths_, format_, path_, nullstr_, header_, delimiter_, quote_, escape_,
    device_, inode_, size_, mtime_sec_, mtime_nsec_, ctime_sec_, ctime_nsec_, columns_);

FileScanDef::FileScanDef()
    : describe_(false), described_columns_(), discovered_paths_(), format_(CSV), path_(), nullstr_(""), header_(true), delimiter_(','), quote_('"'), escape_('"'),
      device_(0), inode_(0), size_(0), mtime_sec_(0), mtime_nsec_(0), ctime_sec_(0), ctime_nsec_(0), columns_()
{}

int FileScanDef::parse(const ParseNode &node, ObSQLSessionInfo &session, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ParseNode *args = nullptr;
  if (OB_FAIL(FileScanReader::check_enabled(session))) {
  } else if (node.num_child_ != 2 || node.children_[0] == nullptr || node.children_[1] == nullptr) {
    ret = OB_ERR_PARSE_SQL;
  } else {
    ObString name = node_text(*node.children_[0]);
    if (name.case_compare("read_csv") == 0) { format_ = CSV; }
    else if (name.case_compare("read_json") == 0 || name.case_compare("read_ndjson") == 0) { format_ = JSONL; }
    else if (name.case_compare("read_parquet") == 0) { format_ = PARQUET; }
    else if (name.case_compare("glob") == 0) { format_ = GLOB; }
    else { ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "unknown file table function"); }
    args = node.children_[1];
  }
  if (OB_SUCC(ret) && (args->num_child_ < 1 || args->children_[0] == nullptr
      || args->children_[0]->type_ != T_VARCHAR)) {
    ret = OB_ERR_PARSE_SQL;
  }
  if (OB_SUCC(ret)) { OZ(ob_write_string(alloc, node_text(*args->children_[0]), path_)); }
  describe_ = node.value_ == 1;
  ObSEArray<ObString, 8> seen;
  for (int64_t i = 1; OB_SUCC(ret) && i < args->num_child_; ++i) {
    const ParseNode *option = args->children_[i];
    const ParseNode *key = nullptr;
    const ParseNode *value = nullptr;
    if (option == nullptr || option->type_ != T_OP_EQ || option->num_child_ != 2
        || option->children_[0] == nullptr || option->children_[1] == nullptr) {
      ret = OB_ERR_PARSE_SQL;
    } else {
      key = option->children_[0]; value = option->children_[1];
      if (key->type_ == T_COLUMN_REF && key->num_child_ == 3 && key->children_[0] == nullptr
          && key->children_[1] == nullptr) { key = key->children_[2]; }
      if (key == nullptr || key->type_ != T_IDENT) { ret = OB_ERR_PARSE_SQL; }
    }
    if (OB_SUCC(ret)) {
      ObString name = node_text(*key);
      for (int64_t j = 0; OB_SUCC(ret) && j < seen.count(); ++j) {
        if (seen.at(j).case_compare(name) == 0) { ret = OB_INVALID_ARGUMENT; }
      }
      OZ(seen.push_back(name));
      if (OB_FAIL(ret)) {
      } else if (name.case_compare("sample_size") == 0) {
        bool all = value->type_ == T_INT && value->value_ == -1;
        all |= value->type_ == T_OP_NEG && value->num_child_ == 1
            && value->children_[0] != nullptr && value->children_[0]->type_ == T_INT
            && value->children_[0]->value_ == 1;
        if (!all) { ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "sample_size other than -1"); }
      } else if (name.case_compare("header") == 0 && format_ == CSV && value->type_ == T_BOOL) {
        header_ = value->value_ != 0;
      } else if (name.case_compare("format") == 0 && format_ == CSV && value->type_ == T_VARCHAR
                 && node_text(*value).case_compare("csv") == 0) {
      } else if (name.case_compare("format") == 0 && format_ == JSONL && value->type_ == T_VARCHAR
                 && node_text(*value).case_compare("newline_delimited") == 0) {
      } else if (format_ == CSV && value->type_ == T_VARCHAR) {
        ObString text = node_text(*value);
        if (name.case_compare("nullstr") == 0) { OZ(ob_write_string(alloc, text, nullstr_)); }
        else if (text.length() != 1 || text[0] == '\0' || text[0] == '\n' || text[0] == '\r') {
          ret = OB_INVALID_ARGUMENT;
        } else if (name.case_compare("delim") == 0 || name.case_compare("delimiter") == 0) { delimiter_ = text[0]; }
        else if (name.case_compare("quote") == 0) { quote_ = text[0]; }
        else if (name.case_compare("escape") == 0) { escape_ = text[0]; }
        else { ret = OB_INVALID_ARGUMENT; }
      } else { ret = OB_INVALID_ARGUMENT; }
    }
  }
  if (OB_SUCC(ret) && delimiter_ == quote_) { ret = OB_INVALID_ARGUMENT; }
  for (int64_t i = 0; OB_SUCC(ret) && i < nullstr_.length(); ++i) {
    if (nullstr_[i] == delimiter_ || nullstr_[i] == quote_ || nullstr_[i] == '\r'
        || nullstr_[i] == '\n' || nullstr_[i] == '\0') { ret = OB_INVALID_ARGUMENT; }
  }
  if (OB_FAIL(ret)) {
    int64_t argument_count = args == nullptr ? -1 : args->num_child_;
    int64_t first_type = argument_count < 1 || args->children_[0] == nullptr ? -1 : args->children_[0]->type_;
    LOG_WARN("invalid file table arguments", K(ret), K(argument_count), K(first_type));
  }
  return ret;
}

int FileScanDef::encode(ObIAllocator &alloc, ObString &value) const
{
  int ret = OB_SUCCESS;
  int64_t size = get_serialize_size();
  char *buf = static_cast<char *>(alloc.alloc(size));
  int64_t pos = 0;
  if (buf == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  else if (OB_FAIL(serialize(buf, size, pos))) {}
  else { value.assign_ptr(buf, static_cast<int32_t>(pos)); }
  return ret;
}

int FileScanDef::decode(const ObString &value)
{
  int64_t pos = 0;
  int ret = deserialize(value.ptr(), value.length(), pos);
  if (OB_SUCC(ret) && pos != value.length()) { ret = OB_INVALID_ARGUMENT; }
  return ret;
}

int64_t FileScanDef::find_column(const ObString &name) const
{
  int64_t index = -1;
  for (int64_t i = 0; index < 0 && i < columns_.count(); ++i) {
    if (columns_.at(i).name_.case_compare(name) == 0) { index = i; }
  }
  return index;
}

int FileScanDef::add_column(const ObString &name, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  FileColumn column;
  ObStringBuffer generated(&alloc);
  if (columns_.count() >= MAX_FILE_COLUMNS) { ret = OB_SIZE_OVERFLOW; }
  else if (name.empty()) { char buffer[64];
    snprintf(buffer, sizeof(buffer), "column%ld", columns_.count());
    OZ(generated.append(buffer)); }
  else { OZ(generated.append(name)); }
  ObString base = generated.string();
  int64_t suffix = 2;
  while (OB_SUCC(ret) && find_column(generated.string()) >= 0) {
    ObStringBuffer next(&alloc);
    OZ(next.append(base)); char buffer[64];
    snprintf(buffer, sizeof(buffer), "__%ld", suffix++); OZ(next.append(buffer));
    generated.reuse(); OZ(generated.append(next.string()));
  }
  OZ(ob_write_string(alloc, generated.string(), column.name_));
  OZ(columns_.push_back(column));
  return ret;
}

FileScanReader::FileScanReader()
    : def_(nullptr), file_(nullptr), parquet_(nullptr), identity_(), row_number_(0), row_alloc_("FileScanRow")
{}
FileScanReader::~FileScanReader() { close(); }

int FileScanReader::check_enabled(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObObj enabled;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {}
  else if (enabled.get_int() == 0) {
    ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled; set enable_file_sql=ON");
  }
  return ret;
}

int FileScanReader::secure_open(const ObString &path, ObSQLSessionInfo &session,
                               ObIAllocator &alloc, ObString &canonical, int &fd, bool directory)
{
  int ret = OB_SUCCESS;
  char input[PATH_MAX];
  char resolved[PATH_MAX];
  fd = -1;
  if (OB_FAIL(check_enabled(session))) {}
  else if (path.empty() || path.length() >= PATH_MAX || memchr(path.ptr(), '\0', path.length()) != nullptr) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(input, path.ptr(), path.length()); input[path.length()] = '\0';
    if (::realpath(input, resolved) == nullptr) { ret = file_error(OB_FILE_NOT_EXIST, "file SQL path does not exist"); }
    else if (OB_FAIL(check_file_scope(session, ObString(resolved)))) {}
    else { OZ(ob_write_string(alloc, ObString(resolved), canonical)); }
  }
  // Walk the canonical path without following symlinks, including parent directories.
  // A replacement between realpath() and open() must never escape the checked tree.
  int current = -1;
  if (OB_SUCC(ret) && (current = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC)) < 0) { ret = OB_IO_ERROR; }
  char *part = resolved + 1;
  while (OB_SUCC(ret) && *part != '\0') {
    char *slash = strchr(part, '/');
    if (slash != nullptr) { *slash = '\0'; }
    int flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW | (slash == nullptr && !directory ? O_NONBLOCK : O_DIRECTORY);
    int next = ::openat(current, part, flags);
    ::close(current); current = next;
    if (current < 0) { ret = file_error(OB_IO_ERROR, "file SQL could not securely open path"); }
    if (slash == nullptr) { break; }
    part = slash + 1;
  }
  struct stat st;
  if (OB_SUCC(ret) && (::fstat(current, &st) != 0 || (directory ? !S_ISDIR(st.st_mode) : !S_ISREG(st.st_mode)))) {
    ret = file_error(OB_INVALID_ARGUMENT, "file SQL requires a regular file");
  }
  if (OB_SUCC(ret)) { fd = current; }
  else if (current >= 0) { ::close(current); }
  return ret;
}

int FileScanReader::open(const FileScanDef &def, ObSQLSessionInfo &session, bool infer)
{
  int ret = OB_SUCCESS;
  close(); def_ = &def;
  if (def.describe_ || def.format_ == FileScanDef::GLOB) {
    ret = check_enabled(session);
  } else {
    int fd = -1;
    ObString canonical;
    if (OB_FAIL(secure_open(def.path_, session, row_alloc_, canonical, fd))) {}
    else if (::fstat(fd, &identity_) != 0) { ret = OB_IO_ERROR; }
    else if (!infer && (def.device_ != static_cast<int64_t>(identity_.st_dev)
        || def.inode_ != static_cast<int64_t>(identity_.st_ino) || def.size_ != identity_.st_size
        || def.mtime_sec_ != identity_.st_mtim.tv_sec || def.mtime_nsec_ != identity_.st_mtim.tv_nsec
        || def.ctime_sec_ != identity_.st_ctim.tv_sec || def.ctime_nsec_ != identity_.st_ctim.tv_nsec)) {
      ret = file_error(OB_INVALID_ARGUMENT, "file SQL input changed after schema inference");
    }
    if (OB_SUCC(ret)) {
      file_ = ::fdopen(fd, "rb");
      if (file_ == nullptr) { ret = OB_IO_ERROR; }
    }
    if (OB_FAIL(ret) && fd >= 0) { ::close(fd); }
    if (OB_SUCC(ret) && def.format_ == FileScanDef::CSV && def.header_ && !infer) {
      ObSEArray<FileCell, 16> cells; OZ(next_csv(cells));
    }
    if (OB_SUCC(ret) && def.format_ == FileScanDef::PARQUET) {
      parquet_ = OB_NEW(FileParquetReader, "FileParquet");
      if (parquet_ == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      else { OZ(parquet_->open(::fileno(file_), def)); }
    }
  }
  return ret;
}

int FileScanReader::project(const ObIArray<int64_t> &columns)
{
  return parquet_ == nullptr ? OB_SUCCESS : parquet_->project(columns);
}

int FileScanReader::check_identity() const
{
  int ret = OB_SUCCESS;
  struct stat now;
  char path[PATH_MAX];
  if (def_ != nullptr && (def_->describe_ || def_->format_ == FileScanDef::GLOB)) {}
  else if (file_ == nullptr || def_ == nullptr) { ret = OB_NOT_INIT; }
  else if (::fstat(::fileno(file_), &now) != 0 || !same_identity(identity_, now)) { ret = OB_INVALID_ARGUMENT; }
  else if (def_->path_.length() >= PATH_MAX) { ret = OB_INVALID_ARGUMENT; }
  else {
    MEMCPY(path, def_->path_.ptr(), def_->path_.length()); path[def_->path_.length()] = '\0';
    if (::lstat(path, &now) != 0 || !same_identity(identity_, now)) { ret = OB_INVALID_ARGUMENT; }
  }
  if (OB_FAIL(ret)) { LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file SQL input changed during scan"); }
  return ret;
}

void FileScanReader::close()
{
  clear_batch();
  if (parquet_ != nullptr) { OB_DELETE(FileParquetReader, "FileParquet", parquet_); parquet_ = nullptr; }
  if (file_ != nullptr) { ::fclose(file_); file_ = nullptr; }
  def_ = nullptr; row_number_ = 0; row_alloc_.reset();
}

int FileScanReader::rescan()
{
  int ret = OB_SUCCESS;
  clear_batch();
  if (OB_FAIL(check_identity())) {}
  else if (parquet_ != nullptr) { ret = parquet_->rescan(); row_number_ = 0; }
  else if (def_->describe_ || def_->format_ == FileScanDef::GLOB) { row_number_ = 0; }
  else if (::fseek(file_, 0, SEEK_SET) != 0) { ret = OB_IO_ERROR; }
  else {
    row_number_ = 0; row_alloc_.reuse();
    if (def_->format_ == FileScanDef::CSV && def_->header_) {
      ObSEArray<FileCell, 16> cells; OZ(next_csv(cells));
    }
  }
  return ret;
}

int FileScanReader::next_csv(ObIArray<FileCell> &cells)
{
  int ret = OB_SUCCESS;
  if (row_number_ == 0) {
    char bom[3];
    size_t count = ::fread(bom, 1, sizeof(bom), file_);
    if (::ferror(file_)) { ret = OB_IO_ERROR; }
    else if (count != sizeof(bom) || MEMCMP(bom, "\xef\xbb\xbf", sizeof(bom)) != 0) {
      if (::fseek(file_, 0, SEEK_SET) != 0) { ret = OB_IO_ERROR; }
    }
  }
  ObStringBuffer field(&row_alloc_);
  bool quoted = false, in_quote = false, after_quote = false, finished = false, any = false;
  int64_t bytes = 0;
  cells.reuse();
  while (OB_SUCC(ret) && !finished) {
    int ch = ::fgetc(file_);
    if (++bytes > MAX_FILE_ROW_BYTES) { ret = OB_SIZE_OVERFLOW; }
    else if (ch == EOF && ::ferror(file_)) { ret = OB_IO_ERROR; }
    else if (in_quote) {
      if (ch == EOF) { ret = OB_INVALID_ARGUMENT; }
      else if (ch == def_->escape_ && def_->escape_ != def_->quote_) {
        int escaped = ::fgetc(file_); ++bytes;
        if (escaped == EOF) { ret = OB_INVALID_ARGUMENT; }
        else { char byte = static_cast<char>(escaped); OZ(field.append(&byte, 1)); }
      } else if (ch == def_->quote_) {
        int next = ::fgetc(file_);
        if (next == def_->quote_ && def_->escape_ == def_->quote_) {
          ++bytes; char byte = static_cast<char>(next); OZ(field.append(&byte, 1));
        } else { in_quote = false; after_quote = true; if (next != EOF) { ::ungetc(next, file_); } }
      } else { char byte = static_cast<char>(ch); OZ(field.append(&byte, 1)); }
    } else if (ch == def_->delimiter_ || ch == '\n' || ch == '\r' || ch == EOF) {
      if (ch == EOF && !any && cells.empty() && field.length() == 0 && !quoted) { ret = OB_ITER_END; }
      else {
        FileCell cell; cell.quoted_ = quoted;
        OZ(ob_write_string(row_alloc_, field.string(), cell.text_));
        cell.null_ = !quoted && cell.text_ == def_->nullstr_;
        OZ(cells.push_back(cell));
        if (cells.count() > MAX_FILE_COLUMNS) { ret = OB_SIZE_OVERFLOW; }
        field.reuse(); quoted = false; after_quote = false;
        finished = ch != def_->delimiter_;
        if (ch == '\r') { int next = ::fgetc(file_); if (next != '\n' && next != EOF) { ::ungetc(next, file_); } }
      }
    } else if (ch == def_->quote_ && field.length() == 0 && !after_quote) {
      quoted = true; in_quote = true;
    } else if (after_quote || ch == def_->quote_) { ret = OB_INVALID_ARGUMENT; }
    else { char byte = static_cast<char>(ch); OZ(field.append(&byte, 1)); }
    any |= ch != EOF;
  }
  if (OB_SUCC(ret)) { ++row_number_; }
  if (ret != OB_ITER_END && OB_FAIL(ret)) {
    LOG_WARN("invalid file SQL CSV record", K(ret), K(row_number_), K(def_->path_));
    record_error(*def_, row_number_ + 1, "malformed CSV record or resource limit exceeded");
  }
  return ret;
}

int FileScanReader::read_line(ObString &line)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&row_alloc_);
  bool finished = false;
  while (OB_SUCC(ret) && !finished) {
    int ch = ::fgetc(file_);
    if (ch == EOF && ::ferror(file_)) { ret = OB_IO_ERROR; }
    else if (ch == EOF || ch == '\n') {
      finished = true;
      if (ch == EOF && buffer.length() == 0) { ret = OB_ITER_END; }
    } else if (buffer.length() >= MAX_FILE_ROW_BYTES) { ret = OB_SIZE_OVERFLOW; }
    else { char byte = static_cast<char>(ch); OZ(buffer.append(&byte, 1)); }
  }
  if (OB_SUCC(ret)) { line = buffer.string(); ++row_number_; }
  return ret;
}

int FileScanReader::next_json(ObIArray<FileCell> &cells, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  ObString line;
  ObJsonNode *node = nullptr;
  cells.reuse(); names.reuse();
  OZ(read_line(line));
  if (OB_SUCC(ret) && OB_FAIL(ObJsonParser::get_tree(&row_alloc_, line, node,
      ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG))) {}
  if (OB_SUCC(ret) && (node == nullptr || node->json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret)) {
    ObJsonObject *object = static_cast<ObJsonObject *>(node);
    if (object->element_count() > MAX_FILE_COLUMNS) { ret = OB_SIZE_OVERFLOW; }
    for (uint64_t i = 0; OB_SUCC(ret) && i < object->element_count(); ++i) {
      ObString name; ObJsonNode *value = nullptr;
      OZ(object->get_key(i, name)); value = object->get_value(i);
      FileCell cell;
      ObJsonBuffer text(&row_alloc_);
      if (OB_FAIL(ret)) {}
      else if (value->json_type() == ObJsonNodeType::J_NULL) { cell.null_ = true; }
      else if (value->json_type() == ObJsonNodeType::J_STRING) {
        ObJsonString *str = static_cast<ObJsonString *>(value);
        cell.text_ = str->get_str();
        cell.quoted_ = true;
      } else if (value->json_type() == ObJsonNodeType::J_OBJECT || value->json_type() == ObJsonNodeType::J_ARRAY) {
        ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested file SQL JSON values");
      } else { OZ(value->print(text, false)); cell.text_ = text.string(); }
      for (int64_t j = 0; OB_SUCC(ret) && j < names.count(); ++j) {
        if (names.at(j).case_compare(name) == 0) { ret = OB_INVALID_ARGUMENT; }
      }
      OZ(names.push_back(name)); OZ(cells.push_back(cell));
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
    LOG_WARN("invalid file SQL JSONL record", K(ret), K(row_number_), K(def_->path_));
    record_error(*def_, row_number_, "expected one flat JSON object per line");
  }
  return ret;
}

int FileScanReader::next(ObIArray<FileCell> &cells, ObIArray<ObString> &names)
{
  clear_batch();
  row_alloc_.reuse();
  return next_record(cells, names);
}

void FileScanReader::clear_batch()
{
  for (int64_t i = 0; i < batch_.count(); ++i) { batch_.at(i)->~FileRecord(); }
  batch_.reuse();
}

int FileScanReader::next_batch(const ObIArray<FileRecord *> *&records)
{
  int ret = OB_SUCCESS;
  clear_batch();
  row_alloc_.reuse();
  records = &batch_;
  bool end = false;
  // Bound the batch by rows and allocation, while allowing one maximum-size record.
  while (OB_SUCC(ret) && !end && batch_.count() < 64
      && (batch_.empty() || row_alloc_.used() < (1L << 20))) {
    void *memory = row_alloc_.alloc(sizeof(FileRecord));
    if (memory == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else {
      FileRecord *record = new(memory) FileRecord();
      ret = next_record(record->cells_, record->names_);
      if (ret == OB_ITER_END) { end = true; ret = OB_SUCCESS; }
      else if (OB_SUCC(ret)) { OZ(batch_.push_back(record)); }
      if (end || OB_FAIL(ret)) { record->~FileRecord(); }
    }
  }
  if (OB_SUCC(ret) && batch_.empty() && end) { ret = OB_ITER_END; }
  return ret;
}

int FileScanReader::next_record(ObIArray<FileCell> &cells, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  cells.reuse(); names.reuse();
  if (OB_FAIL(check_identity())) {}
  else if (def_->describe_ || def_->format_ == FileScanDef::GLOB) { ret = next_metadata(cells); }
  else if (def_->format_ == FileScanDef::CSV) { ret = next_csv(cells); }
  else if (def_->format_ == FileScanDef::JSONL) { ret = next_json(cells, names); }
  else if (parquet_ != nullptr) { ret = parquet_->next(cells, row_alloc_); if (OB_SUCC(ret)) { ++row_number_; } }
  else { ret = OB_NOT_SUPPORTED; }
  for (int64_t i = 0; OB_SUCC(ret) && i < cells.count(); ++i) {
    int64_t valid_bytes = 0;
    const ObString &text = cells.at(i).text_;
    if (!cells.at(i).null_ && (!cells.at(i).typed_ || cells.at(i).value_.is_string_type())) {
      OZ(ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN, text.ptr(), text.length(), valid_bytes));
      if (OB_SUCC(ret) && valid_bytes != text.length()) { ret = OB_INVALID_ARGUMENT; }
    }
  }
  return ret;
}

int FileScanReader::next_metadata(ObIArray<FileCell> &cells)
{
  int ret = OB_SUCCESS;
  int64_t count = def_->describe_ ? def_->described_columns_.count() : def_->discovered_paths_.count();
  if (row_number_ >= count) { ret = OB_ITER_END; }
  else if (!def_->describe_) {
    FileCell cell; cell.text_ = def_->discovered_paths_.at(row_number_++);
    OZ(cells.push_back(cell));
  } else {
    const FileColumn &column = def_->described_columns_.at(row_number_++);
    for (int64_t i = 0; OB_SUCC(ret) && i < 6; ++i) {
      FileCell cell;
      if (i == 0) { cell.text_ = column.name_; }
      else if (i == 1) { cell.text_ = ObString(FileScanUtils::type_name(column.type_)); }
      else if (i == 2) { cell.text_ = ObString(column.nullable_ ? "YES" : "NO"); }
      else { cell.null_ = true; }
      OZ(cells.push_back(cell));
    }
  }
  return ret;
}

static int infer_file_schema(FileScanDef &def, ObSQLSessionInfo &session, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  ObString canonical;
  struct stat st;
  if (OB_FAIL(FileScanReader::secure_open(def.path_, session, alloc, canonical, fd))) {}
  else if (::fstat(fd, &st) != 0) { ret = OB_IO_ERROR; }
  else {
    def.path_ = canonical; def.device_ = st.st_dev; def.inode_ = st.st_ino; def.size_ = st.st_size;
    def.mtime_sec_ = st.st_mtim.tv_sec; def.mtime_nsec_ = st.st_mtim.tv_nsec;
    def.ctime_sec_ = st.st_ctim.tv_sec; def.ctime_nsec_ = st.st_ctim.tv_nsec;
  }
  if (OB_SUCC(ret) && def.format_ == FileScanDef::PARQUET) {
    ret = FileParquetReader::infer(fd, def, alloc);
  }
  if (fd >= 0) { ::close(fd); }
  if (OB_SUCC(ret) && def.format_ != FileScanDef::PARQUET) {
    FileScanReader reader;
    if (OB_SUCC(ret)) { OZ(reader.open(def, session, true)); }
    ObSEArray<FileCell, 16> cells;
    ObSEArray<ObString, 16> names;
    bool first = true;
    int64_t records = 0;
    while (OB_SUCC(ret) && OB_SUCC(ret = reader.next(cells, names))) {
      if ((records & 1023) == 0) { OZ(THIS_WORKER.check_status()); }
      if (def.format_ == FileScanDef::CSV && first) {
        for (int64_t i = 0; OB_SUCC(ret) && i < cells.count(); ++i) {
          ObString name = def.header_ ? cells.at(i).text_ : ObString();
          if (i == 0 && name.length() >= 3 && MEMCMP(name.ptr(), "\xef\xbb\xbf", 3) == 0) {
            name.assign_ptr(name.ptr() + 3, name.length() - 3);
          }
          OZ(def.add_column(name, alloc));
        }
      }
      if (!(def.format_ == FileScanDef::CSV && first && def.header_)) {
        if (def.format_ == FileScanDef::CSV && cells.count() != def.columns_.count()) {
          ret = OB_INVALID_ARGUMENT;
          record_error(def, reader.row_number(), "CSV column count mismatch");
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < cells.count(); ++i) {
          int64_t index = def.format_ == FileScanDef::CSV ? i : def.find_column(names.at(i));
          if (index < 0) {
            OZ(def.add_column(names.at(i), alloc)); index = def.columns_.count() - 1;
            if (OB_SUCC(ret)) { def.columns_.at(index).nullable_ = records > 0; }
          }
          if (OB_SUCC(ret)) {
            FileColumn &column = def.columns_.at(index);
            const FileCell &cell = cells.at(i);
            int64_t type = cell.null_ ? FileColumn::NIL
                : (def.format_ == FileScanDef::JSONL && cell.quoted_ ? FileColumn::TEXT : FileScanUtils::infer_type(cell.text_));
            column.type_ = FileScanUtils::widen(column.type_, type);
            column.nullable_ |= cell.null_;
            column.max_length_ = MAX(column.max_length_, cell.text_.length());
            if (type == FileColumn::DATETIME && cell.text_.length() > 19) {
              column.scale_ = MAX(column.scale_, cell.text_.length() - 20);
            }
          }
        }
        if (def.format_ == FileScanDef::JSONL) {
          for (int64_t i = 0; OB_SUCC(ret) && i < def.columns_.count(); ++i) {
            bool present = false;
            for (int64_t j = 0; j < names.count(); ++j) {
              present |= names.at(j).case_compare(def.columns_.at(i).name_) == 0;
            }
            def.columns_.at(i).nullable_ |= !present;
          }
        }
        ++records;
      }
      first = false;
    }
    if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    if (OB_SUCC(ret)) { OZ(reader.check_identity()); }
    if (OB_SUCC(ret) && def.columns_.empty()) { ret = OB_INVALID_ARGUMENT; }
  }
  return ret;
}

namespace
{
class FileGlobDiscovery
{
public:
  FileGlobDiscovery(FileScanDef &def, ObSQLSessionInfo &session, ObIAllocator &allocator)
      : def_(def), session_(session), allocator_(allocator), entries_(0), path_bytes_(0) {}
  int discover(char *path, const char *remaining, int depth)
  {
    int ret = OB_SUCCESS;
    while (*remaining == '/') { ++remaining; }
    if (depth > 32) { ret = OB_SIZE_OVERFLOW; }
    else if (*remaining == '\0') { OZ(add_file(path)); }
    else {
      const char *slash = strchr(remaining, '/');
      int64_t length = slash == nullptr ? strlen(remaining) : slash - remaining;
      char component[PATH_MAX];
      MEMCPY(component, remaining, length); component[length] = '\0';
      ObArenaAllocator scratch("FileGlobDir");
      ObString canonical;
      int fd = -1;
      OZ(FileScanReader::secure_open(ObString(path), session_, scratch, canonical, fd, true));
      DIR *directory = OB_SUCC(ret) ? ::fdopendir(fd) : nullptr;
      if (OB_SUCC(ret) && directory == nullptr) { ::close(fd); ret = OB_IO_ERROR; }
      int64_t prefix = strlen(path);
      struct dirent *entry = nullptr;
      while (OB_SUCC(ret) && directory != nullptr) {
        errno = 0;
        entry = ::readdir(directory);
        if (entry == nullptr) { if (errno != 0) { ret = OB_IO_ERROR; } break; }
        else if (++entries_ > 100000) { ret = OB_SIZE_OVERFLOW; }
        else if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0
            && ::fnmatch(component, entry->d_name, FNM_PERIOD) == 0) {
          int64_t size = strlen(entry->d_name);
          if (prefix + size + 2 >= PATH_MAX) { ret = OB_SIZE_OVERFLOW; }
          else {
            path[prefix] = '/'; MEMCPY(path + prefix + 1, entry->d_name, size + 1);
            struct stat st;
            if (::lstat(path, &st) != 0) { ret = OB_IO_ERROR; }
            else if (slash == nullptr) { OZ(add_file(path)); }
            else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode)) { OZ(discover(path, slash + 1, depth + 1)); }
            path[prefix] = '\0';
          }
        }
        if ((entries_ & 127) == 0) { OZ(THIS_WORKER.check_status()); }
      }
      if (directory != nullptr) { ::closedir(directory); }
    }
    return ret;
  }
private:
  int add_file(const char *path)
  {
    int ret = OB_SUCCESS;
    struct stat st;
    if (::lstat(path, &st) != 0) { ret = OB_IO_ERROR; }
    else if (!S_ISDIR(st.st_mode)) {
      int fd = -1;
      ObString canonical;
      OZ(FileScanReader::secure_open(ObString(path), session_, allocator_, canonical, fd));
      if (fd >= 0) { ::close(fd); }
      path_bytes_ += canonical.length();
      if (def_.discovered_paths_.count() >= 10000 || path_bytes_ > (8L << 20)) { ret = OB_SIZE_OVERFLOW; }
      OZ(def_.discovered_paths_.push_back(canonical));
    }
    return ret;
  }
  FileScanDef &def_;
  ObSQLSessionInfo &session_;
  ObIAllocator &allocator_;
  int64_t entries_;
  int64_t path_bytes_;
};
}

static int infer_glob(FileScanDef &def, ObSQLSessionInfo &session, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char pattern[PATH_MAX];
  char directory[PATH_MAX];
  const char *remaining = nullptr;
  if (def.path_.empty() || def.path_.length() >= PATH_MAX
      || memchr(def.path_.ptr(), '\0', def.path_.length()) != nullptr) { ret = OB_INVALID_ARGUMENT; }
  else {
    MEMCPY(pattern, def.path_.ptr(), def.path_.length()); pattern[def.path_.length()] = '\0';
    char *wildcard = strpbrk(pattern, "*?[");
    char *slash = wildcard == nullptr ? strrchr(pattern, '/') : wildcard;
    if (wildcard != nullptr) { while (slash > pattern && *slash != '/') { --slash; } }
    if (slash == nullptr || *slash != '/') { strcpy(directory, "."); remaining = pattern; }
    else {
      int64_t length = slash == pattern ? 1 : slash - pattern;
      MEMCPY(directory, pattern, length); directory[length] = '\0'; remaining = slash + 1;
    }
    FileGlobDiscovery discovery(def, session, alloc);
    OZ(discovery.discover(directory, remaining, 0));
  }
  OZ(def.add_column(ObString("file"), alloc));
  if (OB_SUCC(ret)) { def.columns_.at(0).type_ = FileColumn::TEXT; def.columns_.at(0).max_length_ = PATH_MAX; }
  return ret;
}

int FileScanDef::infer(ObSQLSessionInfo &session, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  // Schema inference reads source records, even when the final query describes them.
  bool describe = describe_; describe_ = false;
  if (format_ == GLOB) { ret = infer_glob(*this, session, alloc); }
  else { ret = infer_file_schema(*this, session, alloc); }
  describe_ = describe;
  if (OB_SUCC(ret) && describe_) {
    OZ(described_columns_.assign(columns_));
    columns_.reuse();
    const char *names[] = {"column_name", "column_type", "null", "key", "default", "extra"};
    for (int64_t i = 0; OB_SUCC(ret) && i < 6; ++i) {
      OZ(add_column(ObString(names[i]), alloc));
      if (OB_SUCC(ret)) {
        columns_.at(i).type_ = FileColumn::TEXT;
        columns_.at(i).nullable_ = i >= 3;
        columns_.at(i).max_length_ = 1024;
      }
    }
  }
  return ret;
}

int64_t FileScanUtils::infer_type(const ObString &text)
{
  int64_t type = FileColumn::TEXT;
  if (text.case_compare("true") == 0 || text.case_compare("false") == 0) { type = FileColumn::BOOLEAN; }
  else if (valid_iso_date(text)) { type = FileColumn::DATE; }
  else if (text.length() >= 19 && text.length() <= 26 && (text[10] == 'T' || text[10] == ' ')
      && valid_iso_date(ObString(10, text.ptr())) && text[13] == ':' && text[16] == ':'
      && text[11] >= '0' && text[11] <= '2' && text[12] >= '0' && text[12] <= '9'
      && (text[11]-'0')*10+text[12]-'0' < 24
      && text[14] >= '0' && text[14] <= '5' && text[15] >= '0' && text[15] <= '9'
      && text[17] >= '0' && text[17] <= '5' && text[18] >= '0' && text[18] <= '9') {
    bool valid = text.length() == 19 || (text.length() >= 21 && text[19] == '.');
    for (int64_t i = 20; valid && i < text.length(); ++i) { valid = text[i] >= '0' && text[i] <= '9'; }
    if (valid) { type = FileColumn::DATETIME; }
  }
  else if (text.length() > 0 && text.length() < 128) {
    char number[128]; MEMCPY(number, text.ptr(), text.length()); number[text.length()] = '\0';
    bool integer = true, decimal = true;
    for (int64_t i = (number[0] == '-' || number[0] == '+') ? 1 : 0; i < text.length(); ++i) {
      integer &= number[i] >= '0' && number[i] <= '9';
      decimal &= (number[i] >= '0' && number[i] <= '9') || number[i] == '.'
          || number[i] == 'e' || number[i] == 'E' || number[i] == '+' || number[i] == '-';
    }
    char *end = nullptr; errno = 0;
    if (integer) {
      ::strtoll(number, &end, 10);
      if (errno == 0 && end != number && *end == '\0') { type = FileColumn::INTEGER; }
    } else if (decimal && ((number[0] >= '0' && number[0] <= '9') || number[0] == '-'
        || number[0] == '+' || number[0] == '.')) {
      double value = ::strtod(number, &end);
      if (errno == 0 && end != number && *end == '\0' && ::isfinite(value)) { type = FileColumn::REAL; }
    }
  }
  return type;
}

int64_t FileScanUtils::widen(int64_t left, int64_t right)
{
  int64_t type = FileColumn::TEXT;
  if (left == FileColumn::NIL) { type = right; }
  else if (right == FileColumn::NIL || left == right) { type = left; }
  else if ((left == FileColumn::INTEGER && right == FileColumn::REAL)
      || (right == FileColumn::INTEGER && left == FileColumn::REAL)) { type = FileColumn::REAL; }
  else if ((left == FileColumn::DATE && right == FileColumn::DATETIME)
      || (right == FileColumn::DATE && left == FileColumn::DATETIME)) { type = FileColumn::DATETIME; }
  return type;
}

const char *FileScanUtils::type_name(int64_t type)
{
  const char *names[] = {"NULL", "BIGINT", "DOUBLE", "BOOLEAN", "VARCHAR", "DATE", "DATETIME"};
  return type >= FileColumn::NIL && type <= FileColumn::DATETIME ? names[type] : "VARCHAR";
}
ObObjType FileScanUtils::obj_type(int64_t type)
{
  ObObjType types[] = {ObVarcharType, ObIntType, ObDoubleType, ObTinyIntType, ObVarcharType, ObDateType, ObDateTimeType};
  return type >= FileColumn::NIL && type <= FileColumn::DATETIME ? types[type] : ObVarcharType;
}

int FileScanUtils::convert(const FileCell &cell, const FileColumn &column, ObObj &value)
{
  int ret = OB_SUCCESS;
  const ObString &text = cell.text_;
  if (cell.null_) { value.set_null(); }
  else if (cell.typed_) { value = cell.value_; if (value.is_string_type()) { value.set_collation_type(CS_TYPE_UTF8MB4_BIN); } }
  else if (column.type_ == FileColumn::TEXT || column.type_ == FileColumn::NIL) {
    value.set_varchar(text); value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else if (column.type_ == FileColumn::BOOLEAN) { value.set_tinyint(text.case_compare("true") == 0 ? 1 : 0); }
  else if (column.type_ == FileColumn::DATE) {
    int32_t date = 0; OZ(ObTimeConverter::str_to_date(text, date)); value.set_date(date);
  } else if (column.type_ == FileColumn::DATETIME) {
    int64_t date = 0; ObTimeConvertCtx context(nullptr, false);
    OZ(ObTimeConverter::str_to_datetime(text, context, date)); value.set_datetime(date);
  } else if (text.length() >= 128) { ret = OB_INVALID_ARGUMENT; }
  else {
    char number[128]; MEMCPY(number, text.ptr(), text.length()); number[text.length()] = '\0';
    char *end = nullptr; errno = 0;
    if (column.type_ == FileColumn::INTEGER) { value.set_int(::strtoll(number, &end, 10)); }
    else { value.set_double(::strtod(number, &end)); }
    if (errno != 0 || end == number || *end != '\0') { ret = OB_INVALID_ARGUMENT; }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
