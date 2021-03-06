#include "fastq_chunk.h"
#include "libagd/src/buffer.h"
#include "libagd/src/object_pool.h"
#include "libagd/src/proto/alignment.pb.h"
#include <google/protobuf/io/gzip_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

class FastqChunker {
 public:
  FastqChunker(const char* data, const uint64_t file_size,
               const size_t chunk_size);

  // Assigns a fastq resource to the parameter
  // bool indicates whether this operation was successful
  // false = no more chunks
  bool next_chunk(FastqChunk& chunk);

 private:
  bool create_chunk(FastqChunk& chunk);
  bool advance_record();
  bool advance_line();

  const char* skip_line();

  const char* data_;
  const char *current_ptr_, *end_ptr_;
  std::size_t chunk_size_;
};

class CompressedFastqChunker {
 public:
  CompressedFastqChunker(const size_t chunk_size, agd::ObjectPool<agd::Buffer>* buf_pool) : chunk_size_(chunk_size), buf_pool_(buf_pool) {}
  Status Init(const std::string input_fastq);
  bool next_chunk(BufferedFastqChunk& chunk);

 private:
  size_t chunk_size_;
  agd::ObjectPool<agd::Buffer>* buf_pool_;
  agd::ObjectPool<agd::Buffer>::ptr_type leftover_buf_;
  bool iseof_ = false;

  std::unique_ptr<std::istream> istrm_in_;
  std::unique_ptr<google::protobuf::io::IstreamInputStream> input_strm_;
  std::unique_ptr<google::protobuf::io::GzipInputStream> gzip_in_;
};