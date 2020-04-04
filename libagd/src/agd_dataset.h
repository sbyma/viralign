
#pragma once
#include <tuple>
#include "agd_record_reader.h"
#include "json.hpp"
#include "parser.h"

namespace agd {
using json = nlohmann::json;

typedef std::tuple<uint64_t, uint64_t, std::string> ChunkTuple;

// AGD Dataset loads an entire dataset into memory.
// This may not be suitable for very large datasets.
// Consider adding AGDBufferedDataset, which will buffer one or two chunks
// from each requested column for sequential access.
// TODO redesign this file with a common interface and two implementations
class AGDDataset {
 public:
  // create dataset
  // load only specific columns if given
  static Status Create(const std::string& agd_json_path,
                       std::unique_ptr<AGDDataset>& dataset,
                       std::vector<std::string> columns = {});

  // allows iteration over a complete agd column
  // associated AGDDataset must outlive the ColumnIterator
  class ColumnIterator {
    friend class AGDDataset;

   public:
    ColumnIterator() = default;
    Status GetNextRecord(const char** data, size_t* size);
    Status GetNextAt(size_t index, const char** data, size_t* size);
    void Reset();

   private:
    ColumnIterator(std::vector<AGDRecordReader>* readers,
                   uint32_t total_records)
        : column_readers_(readers), total_records_(total_records) {}

    std::vector<AGDRecordReader>* column_readers_;
    uint32_t current_reader_ = 0;
    uint32_t total_records_;
  };

  Status Column(const std::string& column, ColumnIterator* iter) {
    if (column_map_.find(column) != column_map_.end()) {
      *iter = ColumnIterator(&column_map_[column], total_records_);
      return Status::OK();
    } else {
      return ObjNotFound("column ", column, " not found in column map.");
    }
  }

  uint32_t Size() const { return total_records_; }

  const std::string& Name() const { return name_; }

 private:
  AGDDataset() = default;
  Status Initialize(const std::string& agd_json_path,
                    const std::vector<std::string>& columns);
  std::string name_;

  json agd_metadata_;

  std::vector<Buffer> chunks_;
  // map column name to its set of readers
  std::unordered_map<std::string, std::vector<AGDRecordReader>> column_map_;
  std::vector<size_t> chunk_sizes_;
  uint32_t total_records_;
};

// a buffered dataset that reads chunks from columns on the fly
// reduced memory consumption, but not efficient
// There is fundamentally a trade off between streaming access and random access
class AGDBufferedDataset {
 public:
  static Status Create(const std::string& agd_json_path,
                       std::unique_ptr<AGDBufferedDataset>& dataset,
                       std::vector<std::string> columns = {});

  class ColumnIterator {
    friend class AGDBufferedDataset;

   public:
    ColumnIterator() = default;
    Status GetNextRecord(const char** data, size_t* size);
    Status GetRecordAt(size_t index, const char** data, size_t* size);
    void Reset();

   private:
    ColumnIterator(const std::string& col, uint32_t total_records,
                   uint32_t chunk_size, const std::vector<ChunkTuple>& chunks,
                   const std::string& path_base)
        : column_(col),
          total_records_(total_records),
          chunk_size_(chunk_size),
          chunks_(&chunks),
          file_path_base_(path_base) {}

    Status LoadChunk(const ChunkTuple& chunk);

    std::unique_ptr<AGDRecordReader> current_reader_;
    agd::Buffer current_buf_;
    std::string column_;
    uint32_t total_records_;
    uint32_t chunk_size_;
    const std::vector<ChunkTuple>* chunks_;
    std::string file_path_base_;
    RecordParser parser_;
    uint64_t abs_index_ = 0;
    uint64_t current_chunk_ = 0;
  };

  Status Column(const std::string& column, ColumnIterator* iter) {
    auto it = std::find(columns_.begin(), columns_.end(), column);
    if (it != columns_.end()) {
      *iter = ColumnIterator(column, total_records_, chunk_size_, chunks_, file_path_base_);
      return Status::OK();
    } else {
      return ObjNotFound("column ", column, " not found in column map.");
    }
  }

 private:
  AGDBufferedDataset() = default;
  Status Initialize(const std::string& agd_json_path,
                    const std::vector<std::string>& columns);
  std::string name_;

  json agd_metadata_;

  uint32_t total_records_;
  uint32_t chunk_size_;
  std::vector<ChunkTuple> chunks_;
  std::vector<std::string> columns_;
  std::string file_path_base_;
};

// interface to stream an AGD dataset.
// overlapped IO and parsing with threading
// fast and efficient
// DOES NOT provide random access
// TODO
class AGDStreamingDataset {

};

}  // namespace agd
