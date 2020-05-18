#include "json.hpp"
#include "libagd/src/agd_filesystem_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "parallel_aligner.h"

struct FileSystemManagerParams {
  agd::ReadQueueType* input_queue;
  uint32_t max_records;
  int filter_contig_index;
  GenomeIndex* index;
  AlignerOptions* options;
  size_t aligner_threads;
  size_t reader_threads;
  size_t writer_threads;
  absl::string_view redis_addr;
  absl::string_view queue_name;
};

class FileSystemManager {
 public:
  static errors::Status Run(const FileSystemManagerParams& params);
};