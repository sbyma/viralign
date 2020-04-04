
#include "agd_dataset.h"
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <tuple>
#include "absl/strings/str_cat.h"

namespace agd {

using std::ifstream;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;

Status AGDDataset::Create(const string& agd_json_path,
                          unique_ptr<AGDDataset>& dataset,
                          vector<string> columns) {
  std::cout << "Loading AGD dataset '" << agd_json_path << "' ...\n";
  dataset.reset(new AGDDataset());
  Status s = dataset->Initialize(agd_json_path, columns);
  if (!s.ok()) {
    dataset.reset();
  }

  return s;
}

Status AGDDataset::Initialize(const string& agd_json_path,
                              const vector<string>& columns) {
  ifstream i(agd_json_path);
  json agd_metadata;
  i >> agd_metadata;
  name_ = agd_metadata["name"];
  agd_metadata_ = agd_metadata;

  vector<string> to_load;
  const auto& cols = agd_metadata["columns"];
  if (!columns.empty()) {
    for (const auto& c : columns) {
      bool found = false;
      for (const auto& json_col : cols) {
        if (json_col == c) {
          found = true;
          break;
        }
      }
      if (!found) {
        return NotFound("column ", c, " was not found in dataset.");
      }
    }
    to_load = columns;
  } else {
    for (const auto& c : cols) {
      to_load.push_back(c);
    }
  }

  string file_path_base =
      agd_json_path.substr(0, agd_json_path.find_last_of('/') + 1);

  RecordParser parser;

  // ensure the records from the JSON are in ascending order
  // JSON spec does not guarantee ordering
  vector<ChunkTuple> records(agd_metadata["records"].size());
  for (const auto& chunk : agd_metadata["records"]) {
    records.push_back(std::make_tuple(chunk["first"].get<uint64_t>(),
                                      chunk["last"].get<uint64_t>(),
                                      chunk["path"].get<std::string>()));
  }

  std::sort(records.begin(), records.end(),
            [](const ChunkTuple& a, const ChunkTuple& b) {
              return std::get<0>(a) < std::get<0>(b);
            });

  for (const auto& c : to_load) {
    for (const auto& chunk : records) {
      string chunk_name = std::get<2>(chunk);
      string path = absl::StrCat(file_path_base, chunk_name, ".", c);

      const int fd = open(path.c_str(), O_RDONLY);
      struct stat st;
      if (stat(path.c_str(), &st) != 0) {
        return Internal("Unable to stat file ", path);
      }
      auto size = st.st_size;
      char* mapped = (char*)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
      if (mapped == MAP_FAILED) {
        return Internal("Unable to map file ", path, ", returned ", mapped);
      }

      Buffer chunk_buf(size, 1024 * 1024);
      uint64_t first_ordinal;
      uint32_t num_records;
      string record_id;
      parser.ParseNew(mapped, size, false, &chunk_buf, &first_ordinal,
                      &num_records, record_id);

      column_map_[c].push_back(AGDRecordReader(chunk_buf.data(), num_records));
      chunks_.push_back(std::move(chunk_buf));
      chunk_sizes_.push_back(num_records);
      total_records_ += num_records;

      munmap(mapped, size);
      close(fd);
    }
  }

  return Status::OK();
}

Status AGDDataset::ColumnIterator::GetNextRecord(const char** data,
                                                 size_t* size) {
  Status s = (*column_readers_)[current_reader_].GetNextRecord(data, size);
  if (!s.ok()) {
    if (current_reader_ < column_readers_->size() - 1) {
      current_reader_++;
      return (*column_readers_)[current_reader_].GetNextRecord(data, size);
    } else {
      return OutOfRange("Last record in column");
    }
  }
  return s;
}

Status AGDDataset::ColumnIterator::GetNextAt(size_t index, const char** data,
                                             size_t* size) {
  if (index >= total_records_) {
    return OutOfRange("index is greater than total records in dataset");
  }

  size_t chunk_index = index / (*column_readers_)[0].NumRecords();
  size_t record_index = index % (*column_readers_)[0].NumRecords();
  Status s =
      (*column_readers_)[chunk_index].GetRecordAt(record_index, data, size);
  if (!s.ok()) {
    return OutOfRange("Last record in column");
  }
  return s;
}

void AGDDataset::ColumnIterator::Reset() {
  current_reader_ = 0;
  for (auto reader : *column_readers_) {
    reader.Reset();
  }
}

// AGDBufferedDataset

Status AGDBufferedDataset::Create(const string& agd_json_path,
                                  unique_ptr<AGDBufferedDataset>& dataset,
                                  vector<string> columns) {
  std::cout << "Loading buffered AGD dataset '" << agd_json_path << "' ...\n";
  dataset.reset(new AGDBufferedDataset());
  Status s = dataset->Initialize(agd_json_path, columns);
  if (!s.ok()) {
    dataset.reset();
  }

  return s;
}

Status AGDBufferedDataset::Initialize(const string& agd_json_path,
                                      const vector<string>& columns) {
  ifstream i(agd_json_path);
  json agd_metadata;
  i >> agd_metadata;
  name_ = agd_metadata["name"];
  agd_metadata_ = agd_metadata;

  const auto& cols = agd_metadata["columns"];
  if (!columns.empty()) {
    for (const auto& c : columns) {
      bool found = false;
      for (const auto& json_col : cols) {
        if (json_col == c) {
          found = true;
          break;
        }
      }
      if (!found) {
        return NotFound("column ", c, " was not found in dataset.");
      }
    }
    columns_ = columns;
  } else {
    for (const auto& c : cols) {
      columns_.push_back(c);
    }
  }

  file_path_base_ =
      agd_json_path.substr(0, agd_json_path.find_last_of('/') + 1);

  // ensure the records from the JSON are in ascending order
  // JSON spec does not guarantee ordering
  total_records_ = 0;
  chunks_.reserve(agd_metadata["records"].size());
  for (const auto& chunk : agd_metadata["records"]) {
    chunks_.push_back(std::make_tuple(chunk["first"].get<uint64_t>(),
                                      chunk["last"].get<uint64_t>(),
                                      chunk["path"].get<std::string>()));
    total_records_ +=
        chunk["last"].get<uint64_t>() - chunk["first"].get<uint64_t>();
  }

  std::sort(chunks_.begin(), chunks_.end(),
            [](const ChunkTuple& a, const ChunkTuple& b) {
              return std::get<0>(a) < std::get<0>(b);
            });

  chunk_size_ = std::get<1>(chunks_[0]) - std::get<0>(chunks_[0]);
  std::cout << "chunk size is " << chunk_size_ << "\n";

  return Status::OK();
}

Status AGDBufferedDataset::ColumnIterator::LoadChunk(const ChunkTuple& chunk) {
  string path = absl::StrCat(file_path_base_, std::get<2>(chunk), ".", column_);

  const int fd = open(path.c_str(), O_RDONLY);
  struct stat st;
  if (stat(path.c_str(), &st) != 0) {
    return Internal("Unable to stat file ", path);
  }
  auto size = st.st_size;
  char* mapped = (char*)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    return Internal("Unable to map file ", path, ", returned ", mapped);
  }

  uint64_t first_ordinal;
  uint32_t num_records;
  string record_id;
  ERR_RETURN_IF_ERROR(parser_.ParseNew(mapped, size, false, &current_buf_,
                                       &first_ordinal, &num_records,
                                       record_id));

  current_reader_ =
      std::make_unique<AGDRecordReader>(current_buf_.data(), num_records);

  munmap(mapped, size);
  close(fd);

  return Status::OK();
}

Status AGDBufferedDataset::ColumnIterator::GetNextRecord(const char** data,
                                                         size_t* size) {
  if (!current_reader_.get()) {
    // load the first
    ERR_RETURN_IF_ERROR(LoadChunk(chunks_->at(0)));
    abs_index_ = 0;
    current_chunk_ = 0;
  }

  Status s = current_reader_->GetNextRecord(data, size);
  if (errors::IsResourceExhausted(s)) {
    current_chunk_++;
    if (current_chunk_ == chunks_->size()) {
      return s;  // no more chunks
    } else {
      ERR_RETURN_IF_ERROR(LoadChunk(chunks_->at(current_chunk_)));
      return current_reader_->GetNextRecord(data, size);
    }
  }

  return s;
}

Status AGDBufferedDataset::ColumnIterator::GetRecordAt(size_t index,
                                                       const char** data,
                                                       size_t* size) {
  uint64_t chunk = index / chunk_size_;
  if (current_chunk_ != chunk || !current_reader_.get()) {
    // load the appropriate chunk
    ERR_RETURN_IF_ERROR(LoadChunk(chunks_->at(chunk)));
    abs_index_ = index;
    current_chunk_ = chunk;
  }

  int idx = index % chunk_size_;

  Status s = current_reader_->GetRecordAt(idx, data, size);

  return s;
}

void AGDBufferedDataset::ColumnIterator::Reset() {
  current_reader_.reset(nullptr);
  current_chunk_ = 0;
  abs_index_ = 0;
}

}  // namespace agd
