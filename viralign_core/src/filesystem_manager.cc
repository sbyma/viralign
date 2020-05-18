#include "filesystem_manager.h"
#include "redis_pusher.h"

#include <chrono>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "snap-master/SNAPLib/Bam.h"
#include "snap-master/SNAPLib/Read.h"

using json = nlohmann::json;
using namespace std::chrono_literals;

Status FileSystemManager::Run(const FileSystemManagerParams& params) {

  std::vector<std::string> columns = {"base", "qual"};

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDFileSystemReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemReader::Create(
      columns, params.input_queue, params.reader_threads, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  std::unique_ptr<ParallelAligner> aligner;

  ERR_RETURN_IF_ERROR(ParallelAligner::Create(params.aligner_threads, params.index, params.options,
                                              chunk_queue, params.filter_contig_index, aligner));

  auto aln_queue = aligner->GetOutputQueue();

  std::unique_ptr<agd::AGDFileSystemWriter> writer;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemWriter::Create({"aln"}, aln_queue, 10,
                                                       buf_pool, writer));

  if (params.max_records > 0) {
    while (writer->GetNumWritten() != params.max_records) {
      std::this_thread::sleep_for(500ms);
    }
  } else {
    // else run forever 
    // TODO respond to stop signals
    RedisPusher pusher(writer->GetOutputQueue(), params.redis_addr, params.queue_name);
    pusher.Run(); // never stops
  }

  reader->Stop();
  aligner->Stop();
  writer->Stop();

  return Status::OK();
}