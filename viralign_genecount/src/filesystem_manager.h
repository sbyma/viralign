#include "json.hpp"
#include "libagd/src/agd_filesystem_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "genes.h"

struct FileSystemManagerParams {
  agd::ReadQueueType* input_queue;
  size_t reader_threads;
  absl::string_view output_filename;
  uint32_t max_chunks;
  const IntervalForest* interval_forest;
  const GeneIdMap* genes;
};

class FileSystemManager {
 public:
  static errors::Status Run(const FileSystemManagerParams& params);
};