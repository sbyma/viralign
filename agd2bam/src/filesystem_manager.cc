
#include "filesystem_manager.h"

#include "libagd/src/agd_filesystem_reader.h"


#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "bam_builder.h"

using json = nlohmann::json;
//using namespace std::chrono_literals;

Status FileSystemManager::Run(agd::ReadQueueType* input_queue, size_t threads, size_t chunks, const std::string& agd_metadata_path) {

  std::ifstream i(agd_metadata_path);
  json agd_metadata;
  i >> agd_metadata;
  i.close();
  std::vector<std::string> columns = {"base", "qual", "meta", "aln"};

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDFileSystemReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemReader::Create(
      columns, input_queue, threads, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  BamBuilder bb(chunk_queue, chunks);
  ERR_RETURN_IF_ERROR(bb.Init(agd_metadata, threads));
  ERR_RETURN_IF_ERROR(bb.Run());

  reader->Stop();
  return Status::OK();
}