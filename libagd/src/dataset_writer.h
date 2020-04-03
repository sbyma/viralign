
#pragma once

#include <string>
#include <thread>
#include <vector>

#include "buffer.h"
#include "concurrent_queue/concurrent_queue.h"
#include "format.h"
#include "json.hpp"
#include "liberr/errors.h"
#include "object_pool.h"

namespace agd {

using json = nlohmann::json;
using namespace errors;

// Class providers interface to write an AGD dataset, multithreaded
// chunks to write are added to a queue, threads compress and write chunks in
// parallel you probably dont need more than two or three threads supports
// columns base, qual, meta, aln, (prot?) chunks are expected in order
class DatasetWriter {
 public:
  DatasetWriter() = delete;
  ~DatasetWriter() { Stop(); }

  static Status CreateDatasetWriter(size_t compress_threads,
                                    size_t write_threads,
                                    const std::string& name,
                                    const std::string& path,
                                    const std::vector<std::string>& columns,
                                    std::unique_ptr<DatasetWriter>& writer,
                                    ObjectPool<Buffer>* buf_pool = nullptr);

  // write a chunk for each present column
  Status WriteChunks(std::vector<ObjectPool<Buffer>::ptr_type>& column_bufs,
                     size_t chunk_size, uint64_t first_ordinal);

  void Stop();

 private:
  DatasetWriter(const std::string& path, const std::string& name,
                const std::vector<std::string>& columns)
      : path_(path), name_(name), columns_(columns) {}

  Status Init(size_t compress_threads, size_t write_threads, ObjectPool<Buffer>* buf_pool = nullptr);

  std::vector<std::string> columns_;

  struct FormatValue {
    format::RecordType type;
    format::CompressionType compress_type;
  };

  std::unordered_map<std::string, FormatValue> column_map_;
  std::string name_;
  std::string path_;

  // reuse this for both compress and write queue
  // only diff is write queue expects compressed data
  struct ChunkQueueItem {
    ObjectPool<Buffer>::ptr_type buf;
    size_t chunk_size;
    uint64_t first_ordinal;
    absl::string_view column;
  };

  std::vector<std::thread> compress_threads_;

  std::unique_ptr<ConcurrentQueue<ChunkQueueItem>> chunk_queue_;

  std::vector<std::thread> write_threads_;

  std::unique_ptr<ConcurrentQueue<ChunkQueueItem>> write_queue_;
  // TODO should have separate threadpool for writing vs compressing?
};

}  // namespace agd