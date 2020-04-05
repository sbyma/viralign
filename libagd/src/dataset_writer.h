
#pragma once

#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "buffer_pair.h"
#include "concurrent_queue/concurrent_queue.h"
#include "format.h"
#include "json.hpp"
#include "absl/container/flat_hash_map.h"
#include "liberr/errors.h"
#include "object_pool.h"

namespace agd {

using json = nlohmann::json;
using namespace errors;

// stores the vector of records that goes in the chunk metadata json file
typedef std::vector<nlohmann::json> RecordVec;


// Class providers interface to write an AGD dataset, multithreaded
// chunks to write are added to a queue, threads compress and write chunks in
// parallel you probably dont need more than two or three threads supports
// columns base, qual, meta, aln, (prot?) chunks are expected in order
class DatasetWriter {
 public:
  DatasetWriter() = delete;
  ~DatasetWriter() { if (!done_) Stop(); }

  static Status CreateDatasetWriter(size_t compress_threads,
                                    size_t write_threads,
                                    const std::string& name,
                                    const std::string& path,
                                    const std::vector<std::string>& columns,
                                    std::unique_ptr<DatasetWriter>& writer,
                                    ObjectPool<Buffer>* buf_pool);

  // write a chunk for each present column
  // currently not thread safe, but can be made thread safe
  Status WriteChunks(std::vector<ObjectPool<BufferPair>::ptr_type>& column_bufs,
                     size_t chunk_size, uint64_t first_ordinal);

  // write out the metadata json file (after adding all chunks)
  Status WriteMetadata();

  void Stop();

 private:
  DatasetWriter(const std::string& path, const std::string& name,
                const std::vector<std::string>& columns)
      : path_(path), name_(name), columns_(columns) {}

  Status Init(size_t compress_threads, size_t write_threads, ObjectPool<Buffer>* buf_pool);


  struct FormatValue {
    format::RecordType type;
    format::CompressionType compress_type;
  };

  // this could really just be an array but whatever
  absl::flat_hash_map<absl::string_view, FormatValue> column_map_;

  std::string path_;
  std::string name_;
  std::vector<std::string> columns_;

  struct ChunkQueueItem {
    ObjectPool<BufferPair>::ptr_type buf;
    size_t chunk_size;
    uint64_t first_ordinal;
    absl::string_view column;
  };
  
  struct WriteQueueItem {
    ObjectPool<Buffer>::ptr_type buf;
    size_t chunk_size;
    uint64_t first_ordinal;
    absl::string_view column;
  };

  std::vector<std::thread> compress_threads_;

  std::unique_ptr<ConcurrentQueue<ChunkQueueItem>> chunk_queue_;

  std::vector<std::thread> write_threads_;

  std::unique_ptr<ConcurrentQueue<WriteQueueItem>> write_queue_;

  void write_func();
  void compress_func();
  volatile bool done_ = false;
  volatile bool write_done_ = false;

  ObjectPool<Buffer>* buf_pool_;
  RecordVec records_;
  // TODO should have separate threadpool for writing vs compressing?
};

}  // namespace agd