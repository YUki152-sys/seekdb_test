/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include "sql/engine/basic/ob_file_scan_utils.h"
#include <unistd.h>
#include <sys/stat.h>
#include <cmath>
#include "lib/string/ob_string_buffer.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
const int64_t MAX_PARQUET_METADATA = 16L << 20;
const int64_t MAX_PARQUET_PAGE = 32L << 20;
const int64_t MAX_PARQUET_MEMORY = 128L << 20;
const int64_t MAX_PARQUET_PAGE_VALUES = 1L << 20;

uint32_t little32(const char *data)
{
  const unsigned char *p = reinterpret_cast<const unsigned char *>(data);
  return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8)
      | (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}
uint64_t little64(const char *data)
{
  return static_cast<uint64_t>(little32(data)) | (static_cast<uint64_t>(little32(data + 4)) << 32);
}
int read_at(int fd, char *buffer, int64_t count, int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t done = 0;
  while (OB_SUCC(ret) && done < count) {
    ssize_t got = ::pread(fd, buffer + done, count - done, offset + done);
    if (got < 0 && errno == EINTR) {}
    else if (got <= 0) { ret = OB_IO_ERROR; }
    else { done += got; }
  }
  return ret;
}
int read_varint(const ObString &data, int64_t &pos, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  bool done = false;
  for (int shift = 0; OB_SUCC(ret) && !done && shift < 70; shift += 7) {
    if (pos >= data.length()) { ret = OB_INVALID_ARGUMENT; }
    else {
      uint8_t byte = static_cast<uint8_t>(data[pos++]);
      if (shift == 63 && byte > 1) { ret = OB_INVALID_ARGUMENT; }
      else { value |= static_cast<uint64_t>(byte & 127) << shift; done = (byte & 128) == 0; }
    }
  }
  if (!done && OB_SUCC(ret)) { ret = OB_INVALID_ARGUMENT; }
  return ret;
}
}

struct ParquetValue
{
  ParquetValue(ObIAllocator &allocator)
      : id_(0), type_(0), integer_(0), string_(), children_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator) {}
  const ParquetValue *field(int id) const
  {
    const ParquetValue *value = nullptr;
    for (int64_t i = 0; value == nullptr && i < children_.count(); ++i) {
      if (children_.at(i)->id_ == id) { value = children_.at(i); }
    }
    return value;
  }
  int64_t number(int id, int64_t fallback = -1) const
  {
    const ParquetValue *value = field(id);
    return value != nullptr ? value->integer_ : fallback;
  }
  int64_t id_;
  int64_t type_;
  int64_t integer_;
  ObString string_;
  ObSEArray<ParquetValue *, 4, ObIAllocator &> children_;
};

namespace
{
class ParquetCompact
{
public:
  ParquetCompact(ObIAllocator &allocator, const ObString &data)
      : allocator_(allocator), data_(data), pos_(0), nodes_(0) {}
  int parse(ParquetValue *&root) { return value(12, 0, root); }
  int64_t position() const { return pos_; }
private:
  int value(int type, int depth, ParquetValue *&node)
  {
    int ret = OB_SUCCESS;
    void *memory = nullptr;
    if (depth > 32 || ++nodes_ > 100000) { ret = OB_SIZE_OVERFLOW; }
    else if ((memory = allocator_.alloc(sizeof(ParquetValue))) == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { node = new(memory) ParquetValue(allocator_); node->type_ = type; }
    uint64_t encoded = 0;
    if (OB_FAIL(ret)) {}
    else if (type == 1 || type == 2) { node->integer_ = type == 1 ? 1 : 0; }
    else if (type == 3) {
      if (pos_ >= data_.length()) { ret = OB_INVALID_ARGUMENT; }
      else { node->integer_ = static_cast<int8_t>(data_[pos_++]); }
    } else if (type >= 4 && type <= 6) {
      OZ(read_varint(data_, pos_, encoded));
      node->integer_ = static_cast<int64_t>((encoded >> 1) ^ (0ULL - (encoded & 1)));
    } else if (type == 7) {
      if (data_.length() - pos_ < 8) { ret = OB_INVALID_ARGUMENT; } else { pos_ += 8; }
    } else if (type == 8) {
      OZ(read_varint(data_, pos_, encoded));
      if (OB_SUCC(ret) && encoded > static_cast<uint64_t>(data_.length() - pos_)) { ret = OB_INVALID_ARGUMENT; }
      if (OB_SUCC(ret)) { node->string_.assign_ptr(data_.ptr() + pos_, encoded); pos_ += encoded; }
    } else if (type == 9 || type == 10) { OZ(list(*node, depth)); }
    else if (type == 12) { OZ(structure(*node, depth)); }
    else { ret = OB_NOT_SUPPORTED; }
    return ret;
  }
  int structure(ParquetValue &node, int depth)
  {
    int ret = OB_SUCCESS;
    int64_t previous = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (pos_ >= data_.length()) { ret = OB_INVALID_ARGUMENT; }
      else {
        uint8_t header = static_cast<uint8_t>(data_[pos_++]);
        if (header == 0) { done = true; }
        else {
          int64_t id = previous + (header >> 4);
          if ((header >> 4) == 0) {
            uint64_t encoded = 0; OZ(read_varint(data_, pos_, encoded));
            id = static_cast<int64_t>((encoded >> 1) ^ (0ULL - (encoded & 1)));
          }
          ParquetValue *child = nullptr;
          if (id <= 0 || node.field(id) != nullptr) { ret = OB_INVALID_ARGUMENT; }
          OZ(value(header & 15, depth + 1, child));
          if (OB_SUCC(ret)) { child->id_ = id; OZ(node.children_.push_back(child)); previous = id; }
        }
      }
    }
    return ret;
  }
  int list(ParquetValue &node, int depth)
  {
    int ret = OB_SUCCESS;
    uint64_t count = 0;
    int type = 0;
    if (pos_ >= data_.length()) { ret = OB_INVALID_ARGUMENT; }
    else {
      uint8_t header = static_cast<uint8_t>(data_[pos_++]); type = header & 15; count = header >> 4;
      if (count == 15) { OZ(read_varint(data_, pos_, count)); }
      if (count > 100000) { ret = OB_SIZE_OVERFLOW; }
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ParquetValue *child = nullptr;
      if ((type == 1 || type == 2) && pos_ < data_.length()) { type = static_cast<uint8_t>(data_[pos_++]); }
      OZ(value(type, depth + 1, child)); OZ(node.children_.push_back(child));
    }
    return ret;
  }
  ObIAllocator &allocator_;
  ObString data_;
  int64_t pos_;
  int64_t nodes_;
};

int read_footer(int fd, ObIAllocator &allocator, ParquetValue *&footer)
{
  int ret = OB_SUCCESS;
  struct stat st;
  char trailer[8] = {}, magic[4] = {};
  if (::fstat(fd, &st) != 0 || st.st_size < 12) { ret = OB_INVALID_ARGUMENT; }
  OZ(read_at(fd, magic, 4, 0)); OZ(read_at(fd, trailer, 8, st.st_size - 8));
  int64_t size = little32(trailer);
  if (OB_SUCC(ret) && (MEMCMP(magic, "PAR1", 4) != 0 || MEMCMP(trailer + 4, "PAR1", 4) != 0
      || size <= 0 || size > MAX_PARQUET_METADATA || size > st.st_size - 12)) { ret = OB_INVALID_ARGUMENT; }
  char *data = nullptr;
  if (OB_SUCC(ret) && (data = static_cast<char *>(allocator.alloc(size))) == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  OZ(read_at(fd, data, size, st.st_size - size - 8));
  if (OB_SUCC(ret)) { ParquetCompact parser(allocator, ObString(size, data)); OZ(parser.parse(footer)); }
  return ret;
}

int snappy_decode(const ObString &input, char *output, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0, out = 0;
  uint64_t declared = 0;
  OZ(read_varint(input, pos, declared));
  if (declared != static_cast<uint64_t>(size)) { ret = OB_INVALID_ARGUMENT; }
  while (OB_SUCC(ret) && pos < input.length()) {
    uint8_t tag = static_cast<uint8_t>(input[pos++]);
    int kind = tag & 3;
    uint64_t length = 0, offset = 0;
    if (kind == 0) {
      length = tag >> 2;
      if (length < 60) { ++length; }
      else {
        int bytes = length - 59; length = 0;
        if (input.length() - pos < bytes) { ret = OB_INVALID_ARGUMENT; }
        for (int i = 0; OB_SUCC(ret) && i < bytes; ++i) {
          length |= static_cast<uint64_t>(static_cast<uint8_t>(input[pos++])) << (8 * i);
        }
        ++length;
      }
      if (OB_SUCC(ret) && (length > static_cast<uint64_t>(input.length() - pos)
          || length > static_cast<uint64_t>(size - out))) { ret = OB_INVALID_ARGUMENT; }
      if (OB_SUCC(ret)) { MEMCPY(output + out, input.ptr() + pos, length); pos += length; out += length; }
    } else {
      int bytes = kind == 1 ? 1 : (kind == 2 ? 2 : 4);
      length = kind == 1 ? 4 + ((tag >> 2) & 7) : 1 + (tag >> 2);
      if (input.length() - pos < bytes) { ret = OB_INVALID_ARGUMENT; }
      for (int i = 0; OB_SUCC(ret) && i < bytes; ++i) {
        offset |= static_cast<uint64_t>(static_cast<uint8_t>(input[pos++])) << (8 * i);
      }
      if (kind == 1) { offset |= static_cast<uint64_t>(tag & 224) << 3; }
      if (OB_SUCC(ret) && (offset == 0 || offset > static_cast<uint64_t>(out)
          || length > static_cast<uint64_t>(size - out))) { ret = OB_INVALID_ARGUMENT; }
      for (uint64_t i = 0; OB_SUCC(ret) && i < length; ++i) { output[out] = output[out - offset]; ++out; }
    }
  }
  if (OB_SUCC(ret) && out != size) { ret = OB_INVALID_ARGUMENT; }
  return ret;
}

int decode_runs(const ObString &data, int width, int64_t count, ObIArray<int64_t> &output)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  output.reuse();
  if (width < 0 || width > 32 || count < 0 || count > MAX_PARQUET_PAGE_VALUES) { ret = OB_INVALID_ARGUMENT; }
  while (OB_SUCC(ret) && output.count() < count) {
    uint64_t header = 0; OZ(read_varint(data, pos, header));
    if (OB_SUCC(ret) && header == 0) { ret = OB_INVALID_ARGUMENT; }
    else if (OB_SUCC(ret) && (header & 1) == 0) {
      int bytes = (width + 7) / 8;
      int64_t value = 0;
      if (data.length() - pos < bytes || (header >> 1) > static_cast<uint64_t>(count - output.count() + 7)) {
        ret = OB_INVALID_ARGUMENT;
      }
      for (int i = 0; OB_SUCC(ret) && i < bytes; ++i) { value |= static_cast<int64_t>(static_cast<uint8_t>(data[pos++])) << (8 * i); }
      for (uint64_t i = 0; OB_SUCC(ret) && i < (header >> 1) && output.count() < count; ++i) { OZ(output.push_back(value)); }
    } else if (OB_SUCC(ret)) {
      uint64_t groups = header >> 1;
      if (groups > static_cast<uint64_t>((count - output.count() + 7) / 8)
          || (width > 0 && groups > static_cast<uint64_t>((data.length() - pos) / width))) {
        ret = OB_INVALID_ARGUMENT;
      }
      int64_t bytes = groups * width;
      if (OB_SUCC(ret) && bytes > data.length() - pos) { ret = OB_INVALID_ARGUMENT; }
      for (uint64_t i = 0; OB_SUCC(ret) && i < groups * 8 && output.count() < count; ++i) {
        int64_t value = 0;
        for (int bit = 0; bit < width; ++bit) {
          uint64_t offset = i * width + bit;
          value |= static_cast<int64_t>((static_cast<uint8_t>(data[pos + offset / 8]) >> (offset % 8)) & 1) << bit;
        }
        OZ(output.push_back(value));
      }
      pos += bytes;
    }
  }
  return ret;
}
}

struct ParquetColumnCursor
{
  ParquetColumnCursor()
      : memory_("ParquetColumn"), dictionary_memory_("ParquetDict"), column_(0), physical_(0), logical_(-1),
        optional_(false), codec_(0), offset_(0), end_(0), page_(), page_pos_(0), boolean_bit_(0),
        page_rows_(0), page_index_(0), nonnull_index_(0), encoding_(0), page_bytes_(0), dictionary_bytes_(0), levels_(), indices_(), dictionary_() {}
  ObArenaAllocator memory_;
  ObArenaAllocator dictionary_memory_;
  int64_t column_, physical_, logical_;
  bool optional_;
  int64_t codec_, offset_, end_;
  ObString page_;
  int64_t page_pos_, boolean_bit_, page_rows_, page_index_, nonnull_index_, encoding_;
  int64_t page_bytes_, dictionary_bytes_;
  ObSEArray<int64_t, 16> levels_;
  ObSEArray<int64_t, 16> indices_;
  ObSEArray<FileCell, 16> dictionary_;
};

namespace
{
int parquet_logical_type(const ParquetValue &entry, int64_t &logical)
{
  int ret = OB_SUCCESS;
  logical = entry.number(6);
  const ParquetValue *annotation = entry.field(10);
  if (annotation != nullptr) {
    if (annotation->children_.count() != 1) { ret = OB_INVALID_ARGUMENT; }
    else {
      const ParquetValue &type = *annotation->children_.at(0);
      if (type.id_ == 1) { logical = 0; }
      else if (type.id_ == 6) { logical = 6; }
      else if (type.id_ == 8) {
        const ParquetValue *unit = type.field(2);
        if (unit == nullptr || unit->children_.count() != 1) { ret = OB_INVALID_ARGUMENT; }
        else if (unit->field(1) != nullptr) { logical = 9; }
        else if (unit->field(2) != nullptr) { logical = 10; }
        else { ret = OB_NOT_SUPPORTED; }
      } else if (type.id_ == 10 && type.number(2) == 1
          && (type.number(1) == 8 || type.number(1) == 16 || type.number(1) == 32 || type.number(1) == 64)) {
        logical = 18;
      } else { ret = OB_NOT_SUPPORTED; }
    }
  }
  return ret;
}

int plain_value(ParquetColumnCursor &cursor, FileCell &cell)
{
  int ret = OB_SUCCESS;
  const ObString &page = cursor.page_;
  int64_t &pos = cursor.page_pos_;
  int64_t bytes = cursor.physical_ == 0 ? 1 : ((cursor.physical_ == 1 || cursor.physical_ == 4) ? 4 : 8);
  cell.typed_ = true;
  if (cursor.physical_ == 6) {
    if (page.length() - pos < 4) { ret = OB_INVALID_ARGUMENT; }
    else {
      uint32_t length = little32(page.ptr() + pos); pos += 4;
      if (length > static_cast<uint64_t>(page.length() - pos)) { ret = OB_INVALID_ARGUMENT; }
      else { cell.text_.assign_ptr(page.ptr() + pos, length); cell.value_.set_varchar(cell.text_); pos += length; }
    }
  } else if (page.length() - pos < bytes) { ret = OB_INVALID_ARGUMENT; }
  else if (cursor.physical_ == 0) {
    cell.value_.set_tinyint((static_cast<uint8_t>(page[pos]) >> cursor.boolean_bit_) & 1);
    if (++cursor.boolean_bit_ == 8) { cursor.boolean_bit_ = 0; ++pos; }
  } else if (cursor.physical_ == 1) {
    int32_t value = static_cast<int32_t>(little32(page.ptr() + pos)); pos += 4;
    if (cursor.logical_ == 6) {
      if (value < -719162 || value > 2932896) { ret = OB_INVALID_ARGUMENT; }
      else { cell.value_.set_date(value); }
    } else { cell.value_.set_int(value); }
  } else if (cursor.physical_ == 2) {
    int64_t value = static_cast<int64_t>(little64(page.ptr() + pos)); pos += 8;
    if (cursor.logical_ == 9 || cursor.logical_ == 10) {
      if (cursor.logical_ == 9 && (value > INT64_MAX / 1000 || value < INT64_MIN / 1000)) { ret = OB_INVALID_ARGUMENT; }
      else {
        int64_t timestamp = cursor.logical_ == 9 ? value * 1000 : value;
        if (timestamp < -62135596800000000L || timestamp > 253402300799999999L) { ret = OB_INVALID_ARGUMENT; }
        else { cell.value_.set_datetime(timestamp); }
      }
    } else { cell.value_.set_int(value); }
  } else if (cursor.physical_ == 4) {
    uint32_t bits = little32(page.ptr() + pos); float value = 0; MEMCPY(&value, &bits, 4); pos += 4;
    if (!std::isfinite(value)) { ret = OB_INVALID_ARGUMENT; } else { cell.value_.set_double(value); }
  } else if (cursor.physical_ == 5) {
    uint64_t bits = little64(page.ptr() + pos); double value = 0; MEMCPY(&value, &bits, 8); pos += 8;
    if (!std::isfinite(value)) { ret = OB_INVALID_ARGUMENT; } else { cell.value_.set_double(value); }
  } else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

int parquet_schema(const ParquetValue &footer, FileScanDef &def, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ParquetValue *schema = footer.field(2);
  if (schema == nullptr || schema->children_.empty() || schema->children_.count() > 1025) { ret = OB_INVALID_ARGUMENT; }
  else if (schema->children_.at(0)->number(5) != schema->children_.count() - 1) { ret = OB_NOT_SUPPORTED; }
  for (int64_t i = 1; OB_SUCC(ret) && i < schema->children_.count(); ++i) {
    const ParquetValue &entry = *schema->children_.at(i);
    const ParquetValue *name = entry.field(4);
    int64_t physical = entry.number(1), logical = -1, repetition = entry.number(3);
    OZ(parquet_logical_type(entry, logical));
    if (OB_FAIL(ret)) {}
    else if (name == nullptr || entry.field(5) != nullptr || repetition < 0 || repetition > 1
        || (logical != -1 && logical != 0 && logical != 6 && logical != 9 && logical != 10
            && (logical < 15 || logical > 18))
        || (logical == 0 && physical != 6) || (logical == 6 && physical != 1)
        || ((logical == 9 || logical == 10) && physical != 2)
        || (logical >= 15 && logical <= 18 && physical != 1 && physical != 2)) { ret = OB_NOT_SUPPORTED; }
    else {
      OZ(def.add_column(name->string_, allocator));
      if (OB_SUCC(ret)) {
        FileColumn &column = def.columns_.at(i - 1);
        column.nullable_ = repetition == 1;
        if (logical == 6) { column.type_ = FileColumn::DATE; }
        else if (logical == 9 || logical == 10) {
          column.type_ = FileColumn::DATETIME;
          column.scale_ = logical == 9 ? 3 : 6;
        }
        else if (physical == 0) { column.type_ = FileColumn::BOOLEAN; }
        else if (physical == 1 || physical == 2) { column.type_ = FileColumn::INTEGER; }
        else if (physical == 4 || physical == 5) { column.type_ = FileColumn::REAL; }
        else if (physical == 6) { column.type_ = FileColumn::TEXT; column.max_length_ = MAX_PARQUET_PAGE; }
        else { ret = OB_NOT_SUPPORTED; }
      }
    }
  }
  return ret;
}
}

FileParquetReader::FileParquetReader()
    : fd_(-1), def_(nullptr), metadata_("ParquetFooter"), group_memory_("ParquetGroup"), footer_(nullptr), projection_(), cursors_(),
      group_index_(-1), row_in_group_(0), group_rows_(0), allocated_page_bytes_(0)
{}
FileParquetReader::~FileParquetReader() { close(); }

int FileParquetReader::infer(int fd, FileScanDef &def, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator footer_allocator("ParquetInfer");
  ParquetValue *footer = nullptr;
  OZ(read_footer(fd, footer_allocator, footer));
  if (OB_SUCC(ret)) { OZ(parquet_schema(*footer, def, allocator)); }
  return ret;
}

int FileParquetReader::open(int fd, const FileScanDef &def)
{
  int ret = OB_SUCCESS;
  close(); fd_ = fd; def_ = &def;
  OZ(read_footer(fd_, metadata_, footer_));
  if (OB_SUCC(ret) && (footer_->field(4) == nullptr || footer_->field(2) == nullptr)) { ret = OB_INVALID_ARGUMENT; }
  return ret;
}
int FileParquetReader::project(const ObIArray<int64_t> &columns)
{
  int ret = OB_SUCCESS;
  projection_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (columns.at(i) < 0 || columns.at(i) >= def_->columns_.count()) { ret = OB_INVALID_ARGUMENT; }
    else { OZ(projection_.push_back(columns.at(i))); }
  }
  return ret;
}
void FileParquetReader::close()
{
  for (int64_t i = 0; i < cursors_.count(); ++i) { cursors_.at(i)->~ParquetColumnCursor(); }
  cursors_.reset(); projection_.reset(); group_memory_.reset(); metadata_.reset(); footer_ = nullptr; fd_ = -1; def_ = nullptr;
  group_index_ = -1; row_in_group_ = 0; group_rows_ = 0; allocated_page_bytes_ = 0;
}
int FileParquetReader::rescan()
{
  for (int64_t i = 0; i < cursors_.count(); ++i) { cursors_.at(i)->~ParquetColumnCursor(); }
  cursors_.reuse(); group_memory_.reuse(); group_index_ = -1; row_in_group_ = 0; group_rows_ = 0; allocated_page_bytes_ = 0;
  return OB_SUCCESS;
}

int FileParquetReader::open_group()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < cursors_.count(); ++i) { cursors_.at(i)->~ParquetColumnCursor(); }
  cursors_.reuse(); group_memory_.reuse(); allocated_page_bytes_ = 0; row_in_group_ = 0; group_rows_ = 0;
  const ParquetValue *groups = footer_->field(4);
  if (++group_index_ >= groups->children_.count()) { ret = OB_ITER_END; }
  else {
    const ParquetValue &group = *groups->children_.at(group_index_);
    const ParquetValue *chunks = group.field(1);
    const ParquetValue *schema = footer_->field(2);
    group_rows_ = group.number(3);
    if (chunks == nullptr || chunks->children_.count() != def_->columns_.count() || group_rows_ < 0) { ret = OB_INVALID_ARGUMENT; }
    for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) {
      int64_t index = projection_.at(i);
      const ParquetValue &chunk = *chunks->children_.at(index);
      const ParquetValue *metadata = chunk.field(3);
      const ParquetValue &entry = *schema->children_.at(index + 1);
      if (metadata == nullptr || chunk.field(1) != nullptr) { ret = OB_NOT_SUPPORTED; }
      else {
        void *memory = group_memory_.alloc(sizeof(ParquetColumnCursor));
        if (memory == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
        else {
          ParquetColumnCursor *cursor = new(memory) ParquetColumnCursor();
          OZ(cursors_.push_back(cursor));
          cursor->column_ = index; cursor->physical_ = metadata->number(1);
          OZ(parquet_logical_type(entry, cursor->logical_));
          cursor->optional_ = entry.number(3) == 1; cursor->codec_ = metadata->number(4);
          cursor->offset_ = metadata->number(11, metadata->number(9));
          int64_t bytes = metadata->number(7);
          if (cursor->physical_ != entry.number(1) || cursor->offset_ < 4 || bytes < 0 || bytes > def_->size_ - cursor->offset_
              || metadata->number(5) != group_rows_ || (cursor->codec_ != 0 && cursor->codec_ != 1)) {
            ret = OB_NOT_SUPPORTED;
          } else { cursor->end_ = cursor->offset_ + bytes; }
        }
      }
    }
  }
  return ret;
}

int FileParquetReader::decode_page(ParquetColumnCursor &cursor, const ParquetValue &header,
                                   char *decoded, int64_t uncompressed)
{
  int ret = OB_SUCCESS;
  int64_t type = header.number(1);
  const ParquetValue *detail = header.field(type == 2 ? 7 : (type == 3 ? 8 : 5));
  if (detail == nullptr) { ret = OB_NOT_SUPPORTED; }
  else if (type == 2) {
    int64_t count = detail->number(1);
    if (count < 0 || count > MAX_PARQUET_PAGE_VALUES || detail->number(2) != 0) { ret = OB_NOT_SUPPORTED; }
    allocated_page_bytes_ -= cursor.dictionary_bytes_;
    cursor.dictionary_bytes_ = uncompressed + count * sizeof(FileCell);
    allocated_page_bytes_ += cursor.dictionary_bytes_;
    if (allocated_page_bytes_ > MAX_PARQUET_MEMORY) { ret = OB_SIZE_OVERFLOW; }
    cursor.dictionary_.reuse(); cursor.dictionary_memory_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      FileCell cell; OZ(plain_value(cursor, cell));
      if (OB_SUCC(ret) && cell.value_.is_string_type()) {
        OZ(ob_write_string(cursor.dictionary_memory_, cell.text_, cell.text_)); cell.value_.set_varchar(cell.text_);
      }
      OZ(cursor.dictionary_.push_back(cell));
    }
  } else if (type == 0 || type == 3) {
    cursor.page_rows_ = detail->number(1); cursor.encoding_ = detail->number(type == 3 ? 4 : 2);
    if (type == 3 && (detail->number(6) != 0 || detail->number(3) != cursor.page_rows_)) { ret = OB_NOT_SUPPORTED; }
    if (cursor.page_rows_ <= 0 || cursor.page_rows_ > MAX_PARQUET_PAGE_VALUES) { ret = OB_SIZE_OVERFLOW; }
    if (OB_SUCC(ret)) {
      int64_t vectors_memory = cursor.page_rows_ * 2 * sizeof(int64_t);
      if (allocated_page_bytes_ + vectors_memory > MAX_PARQUET_MEMORY) { ret = OB_SIZE_OVERFLOW; }
      else { cursor.page_bytes_ += vectors_memory; allocated_page_bytes_ += vectors_memory; }
    }
    if (OB_SUCC(ret) && cursor.optional_) {
      int64_t prefix = type == 3 ? 0 : 4;
      if (uncompressed < prefix) { ret = OB_INVALID_ARGUMENT; }
      else {
        int64_t bytes = type == 3 ? detail->number(5) : little32(decoded);
        if (bytes < 0 || bytes > uncompressed - prefix || (type == 0 && detail->number(3) != 3)) { ret = OB_NOT_SUPPORTED; }
        else { OZ(decode_runs(ObString(bytes, decoded + prefix), 1, cursor.page_rows_, cursor.levels_)); cursor.page_pos_ = prefix + bytes; }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cursor.levels_.count(); ++i) {
        if (cursor.levels_.at(i) != 0 && cursor.levels_.at(i) != 1) { ret = OB_INVALID_ARGUMENT; }
      }
    }
    if (OB_SUCC(ret) && (cursor.encoding_ == 2 || cursor.encoding_ == 8
        || (cursor.encoding_ == 3 && cursor.physical_ == 0))) {
      int64_t count = cursor.page_rows_;
      if (cursor.optional_) {
        count = 0; for (int64_t i = 0; i < cursor.levels_.count(); ++i) { count += cursor.levels_.at(i); }
      }
      if (count == 0 && cursor.page_pos_ == uncompressed) {}
      else if (cursor.page_pos_ >= uncompressed) { ret = OB_INVALID_ARGUMENT; }
      else if (cursor.encoding_ == 3) {
        if (uncompressed - cursor.page_pos_ < 4) { ret = OB_INVALID_ARGUMENT; }
        else {
          int64_t bytes = little32(decoded + cursor.page_pos_); cursor.page_pos_ += 4;
          if (bytes > uncompressed - cursor.page_pos_) { ret = OB_INVALID_ARGUMENT; }
          else { OZ(decode_runs(ObString(bytes, decoded + cursor.page_pos_), 1, count, cursor.indices_)); }
          for (int64_t i = 0; OB_SUCC(ret) && i < cursor.indices_.count(); ++i) {
            if (cursor.indices_.at(i) != 0 && cursor.indices_.at(i) != 1) { ret = OB_INVALID_ARGUMENT; }
          }
        }
      }
      else {
        int width = static_cast<uint8_t>(decoded[cursor.page_pos_++]);
        OZ(decode_runs(ObString(uncompressed - cursor.page_pos_, decoded + cursor.page_pos_), width, count, cursor.indices_));
      }
    } else if (OB_SUCC(ret) && cursor.encoding_ != 0) { ret = OB_NOT_SUPPORTED; }

  } else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

int FileParquetReader::load_page(ParquetColumnCursor &cursor)
{
  int ret = OB_SUCCESS;
  bool ready = false;
  while (OB_SUCC(ret) && !ready) {
    allocated_page_bytes_ -= cursor.page_bytes_; cursor.page_bytes_ = 0;
    cursor.memory_.reuse(); cursor.levels_.reuse(); cursor.indices_.reuse();
    cursor.page_index_ = 0; cursor.nonnull_index_ = 0; cursor.boolean_bit_ = 0;
    if (cursor.offset_ >= cursor.end_) { ret = OB_INVALID_ARGUMENT; }
    ObArenaAllocator header_allocator("ParquetPageHdr");
    int64_t available = MIN(65536, cursor.end_ - cursor.offset_);
    char *header = nullptr;
    if (OB_SUCC(ret) && (header = static_cast<char *>(header_allocator.alloc(available))) == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    OZ(read_at(fd_, header, available, cursor.offset_));
    ParquetValue *page_header = nullptr;
    int64_t header_bytes = 0;
    if (OB_SUCC(ret)) {
      ParquetCompact parser(header_allocator, ObString(available, header));
      OZ(parser.parse(page_header)); header_bytes = parser.position();
    }
    int64_t compressed = 0, uncompressed = 0, type = -1;
    if (OB_SUCC(ret)) {
      type = page_header->number(1); uncompressed = page_header->number(2); compressed = page_header->number(3);
      if (compressed < 0 || uncompressed < 0 || compressed > MAX_PARQUET_PAGE || uncompressed > MAX_PARQUET_PAGE
          || compressed > cursor.end_ - cursor.offset_ - header_bytes
          || allocated_page_bytes_ + compressed + uncompressed > MAX_PARQUET_MEMORY) { ret = OB_SIZE_OVERFLOW; }
    }
    char *data = nullptr, *decoded = nullptr;
    if (OB_SUCC(ret)) {
      data = static_cast<char *>(cursor.memory_.alloc(MAX(1, compressed)));
      decoded = static_cast<char *>(cursor.memory_.alloc(MAX(1, uncompressed)));
      if (data == nullptr || decoded == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    }
    OZ(read_at(fd_, data, compressed, cursor.offset_ + header_bytes));
    if (OB_SUCC(ret)) {
      if (cursor.codec_ == 0) {
        if (compressed != uncompressed) { ret = OB_INVALID_ARGUMENT; }
        else { MEMCPY(decoded, data, uncompressed); }
      } else if (type == 3) {
        const ParquetValue *detail = page_header->field(8);
        int64_t levels = detail == nullptr ? -1 : detail->number(5) + detail->number(6);
        if (levels < 0 || levels > compressed || levels > uncompressed) { ret = OB_INVALID_ARGUMENT; }
        else if (detail->number(7, 1) == 0) {
          if (compressed != uncompressed) { ret = OB_INVALID_ARGUMENT; }
          else { MEMCPY(decoded, data, uncompressed); }
        } else {
          MEMCPY(decoded, data, levels);
          OZ(snappy_decode(ObString(compressed - levels, data + levels), decoded + levels, uncompressed - levels));
        }
      } else { OZ(snappy_decode(ObString(compressed, data), decoded, uncompressed)); }
    }
    if (OB_SUCC(ret)) {
      cursor.page_bytes_ = compressed + uncompressed;
      allocated_page_bytes_ += cursor.page_bytes_;
      cursor.offset_ += header_bytes + compressed;
      cursor.page_.assign_ptr(decoded, uncompressed); cursor.page_pos_ = 0;
      OZ(decode_page(cursor, *page_header, decoded, uncompressed));
      ready = type == 0 || type == 3;
    }
  }
  return ret;
}

int FileParquetReader::next_cell(ParquetColumnCursor &cursor, FileCell &cell, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (cursor.page_index_ >= cursor.page_rows_) { OZ(load_page(cursor)); }
  if (OB_SUCC(ret)) {
    if (cursor.optional_ && cursor.levels_.at(cursor.page_index_) == 0) { cell.null_ = true; }
    else if (cursor.encoding_ == 3 && cursor.physical_ == 0) {
      if (cursor.nonnull_index_ >= cursor.indices_.count()) { ret = OB_INVALID_ARGUMENT; }
      else { cell.typed_ = true; cell.value_.set_tinyint(cursor.indices_.at(cursor.nonnull_index_++)); }
    }
    else if (cursor.encoding_ == 2 || cursor.encoding_ == 8) {
      if (cursor.nonnull_index_ >= cursor.indices_.count()) { ret = OB_INVALID_ARGUMENT; }
      else {
        int64_t index = cursor.indices_.at(cursor.nonnull_index_++);
        if (index < 0 || index >= cursor.dictionary_.count()) { ret = OB_INVALID_ARGUMENT; }
        else { cell = cursor.dictionary_.at(index); }
      }
    } else { OZ(plain_value(cursor, cell)); }
    if (OB_SUCC(ret) && !cell.null_ && cell.typed_ && cell.value_.is_string_type()) {
      OZ(ob_write_string(allocator, cell.text_, cell.text_)); cell.value_.set_varchar(cell.text_);
    }
    ++cursor.page_index_;
  }
  return ret;
}

int FileParquetReader::next(ObIArray<FileCell> &cells, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && (group_index_ < 0 || row_in_group_ >= group_rows_)) { ret = open_group(); }
  if (OB_SUCC(ret)) {
    cells.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < def_->columns_.count(); ++i) { FileCell missing; missing.null_ = true; OZ(cells.push_back(missing)); }
    for (int64_t i = 0; OB_SUCC(ret) && i < cursors_.count(); ++i) {
      FileCell cell; OZ(next_cell(*cursors_.at(i), cell, allocator));
      if (OB_SUCC(ret)) { cells.at(cursors_.at(i)->column_) = cell; }
    }
    ++row_in_group_;
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
