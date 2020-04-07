#include "ceph_manager.h"

#include <chrono>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "json.hpp"
#include "libagd/src/agd_ceph_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "snap-master/SNAPLib/Bam.h"
#include "snap-master/SNAPLib/Read.h"

using json = nlohmann::json;
using namespace std::chrono_literals;
using namespace errors;

Status CephManager::Run(absl::string_view agd_meta_path,
                        absl::string_view ceph_config_json_path,
                        int filter_contig_index, GenomeIndex* index,
                        AlignerOptions* options) {
  std::ifstream i(agd_meta_path.data());
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  std::ifstream ci(ceph_config_json_path.data());
  json ceph_config_json;
  i >> ceph_config_json;
  ci.close();

  const auto& records = agd_metadata["records"];

  auto file_path_base =
      agd_meta_path.substr(0, agd_meta_path.find_last_of('/') + 1);

  std::cout << "base path is " << file_path_base << "\n";

  std::unique_ptr<agd::AGDCephReader::InputQueueType> input_queue =
      std::make_unique<agd::AGDCephReader::InputQueueType>(5);

  std::vector<std::string> columns = {"base", "qual"};

  const std::string& username = ceph_config_json["client"];
  const std::string& cluster_name = ceph_config_json["cluster"];
  const std::string& name_space = ceph_config_json["namespace"];
  const std::string& ceph_conf_file = ceph_config_json["conf_file"];

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDCephReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDCephReader::Create(
      columns, cluster_name, username, name_space, ceph_conf_file,
      input_queue.get(), 4, buf_pool, reader));

  // std::cout << "[align-core] getting output queue \n";
  auto chunk_queue = reader->GetOutputQueue();

  std::unique_ptr<ParallelAligner> aligner;

  // TODO filter contig index corresponding to SarsCov2
  ERR_RETURN_IF_ERROR(ParallelAligner::Create(/*threads*/ 1, index, options,
                                              chunk_queue, filter_contig_index, aligner));

  auto aln_queue = aligner->GetOutputQueue();

  // TODO convert to AGDCephWriter
  std::unique_ptr<agd::AGDFileSystemWriter> writer;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemWriter::Create({"aln"}, aln_queue, 10,
                                                       buf_pool, writer));

  // dump files for one dataset
  // eventually, we move to continuous reading from redis queue
  for (const auto& rec : records) {
    agd::ReadQueueItem item;

    item.objName = absl::StrCat(file_path_base, rec["path"].get<std::string>());

    item.pool = "lauzhack";  // TODO will be replaced when redis reading works,
                             // or taken from the agd_metadata file
    std::cout << "chunk path is: " << item.objName
              << ", with pool name: " << item.pool << "\n";

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