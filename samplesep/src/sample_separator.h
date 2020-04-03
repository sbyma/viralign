
#include <unordered_map>
#include "libagd/src/buffer.h"
#include "libagd/src/dataset_writer.h"
#include "libagd/src/object_pool.h"
#include "liberr/errors.h"
#include "fastq_parser.h"


using namespace errors;
class SampleSeparator {
 public:
  SampleSeparator(FastqParser* fastq_parser, size_t chunk_size);

  Status Separate();

 private:

  struct SampleChunk {
    agd::ObjectPool<agd::Buffer>::ptr_type base_buf;
    agd::ObjectPool<agd::Buffer>::ptr_type qual_buf;
    agd::ObjectPool<agd::Buffer>::ptr_type meta_buf;
    uint64_t first_ordinal;
    size_t chunk_size;
  };

  std::unordered_map<std::string, SampleChunk> sample_map_;
  std::unordered_map<std::string, std::unique_ptr<agd::DatasetWriter>> writer_map_;

  FastqParser* fastq_parser_;
  agd::ObjectPool<agd::Buffer> buf_pool_;

  size_t chunk_size_;
};