#include "filesystem_manager.h"

#include <chrono>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"

#include "libagd/src/agd_record_reader.h"

#include "genecount.h"

using json = nlohmann::json;
using namespace std::chrono_literals;

Status FileSystemManager::Run(const FileSystemManagerParams& params) {
  std::vector<std::string> columns = {"aln"};

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDFileSystemReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemReader::Create(
      columns, params.input_queue, params.reader_threads, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  Status s = CountGenes(params.max_chunks, chunk_queue, params.interval_forest, *params.genes);

  reader->Stop();

  return s;
}