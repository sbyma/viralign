#include "liberr/errors.h"
#include "json.hpp"
#include "libagd/src/agd_filesystem_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "libagd/src/agd_record_reader.h"
#include "parallel_aligner.h"

class FileSystemManager {
  public:
    static errors::Status Run(absl::string_view agd_meta_path, GenomeIndex* index, AlignerOptions* options);
};