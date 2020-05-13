#include "json.hpp"
#include "libagd/src/agd_filesystem_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "parallel_aligner.h"

class FileSystemManager {
 public:
  static errors::Status Run(agd::ReadQueueType* input_queue, uint32_t max_records,
                            int filter_contig_index, GenomeIndex* index,
                            AlignerOptions* options, size_t threads);
};