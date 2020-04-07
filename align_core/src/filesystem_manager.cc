#include "filesystem_manager.h"

#include <chrono>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "snap-master/SNAPLib/Bam.h"
#include "snap-master/SNAPLib/Read.h"

using json = nlohmann::json;
using namespace std::chrono_literals;

Status FileSystemManager::Run(absl::string_view agd_meta_path,
                              int filter_contig_index, GenomeIndex* index,
                              AlignerOptions* options) {
  std::ifstream i(agd_meta_path.data());
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  const auto& records = agd_metadata["records"];

  auto file_path_base =
      agd_meta_path.substr(0, agd_meta_path.find_last_of('/') + 1);

  std::cout << "base path is " << file_path_base << "\n";

  std::unique_ptr<agd::AGDFileSystemReader::InputQueueType> input_queue =
      std::make_unique<agd::AGDFileSystemReader::InputQueueType>(5);

  std::vector<std::string> columns = {"base", "qual"};

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDFileSystemReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemReader::Create(
      columns, input_queue.get(), 4, buf_pool, reader));

  // std::cout << "[align-core] getting output queue \n";
  auto chunk_queue = reader->GetOutputQueue();

  std::unique_ptr<ParallelAligner> aligner;

  // TODO filter contig index corresponding to SarsCov2
  ERR_RETURN_IF_ERROR(ParallelAligner::Create(/*threads*/ 1, index, options,
                                              chunk_queue, filter_contig_index, aligner));

  auto aln_queue = aligner->GetOutputQueue();

  std::unique_ptr<agd::AGDFileSystemWriter> writer;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemWriter::Create({"aln"}, aln_queue, 10,
                                                       buf_pool, writer));

  // dump files for one dataset
  // eventually, we move to continuous reading from redis queue
  for (const auto& rec : records) {
    agd::ReadQueueItem item;

    item.objName = absl::StrCat(file_path_base, rec["path"].get<std::string>());
    std::cout << "chunk path is: " << item.objName << "\n";

    input_queue->push(std::move(item));
  }

  while (writer->GetNumWritten() != records.size()) {
    std::this_thread::sleep_for(500ms);
  }

  reader->Stop();
  aligner->Stop();
  writer->Stop();

  // if it's fresh (not aligned), add the column to the AGD metadata json
  const auto& cols = agd_metadata["columns"];
  if (std::find(cols.begin(), cols.end(), "aln") == cols.end()) {
    agd_metadata["columns"].push_back("aln");
    std::ofstream o(agd_meta_path.data());
    o << std::setw(4) << agd_metadata;
  }

  return Status::OK();
}