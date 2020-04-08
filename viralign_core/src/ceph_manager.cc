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

Status CephManager::Run(agd::ReadQueueType* input_queue, uint32_t max_records,
                        absl::string_view ceph_config_json_path,
                        int filter_contig_index, GenomeIndex* index,
                        AlignerOptions* options, size_t threads) {
  
  std::ifstream ci(ceph_config_json_path.data());
  json ceph_config_json;
  ci >> ceph_config_json;
  ci.close();

  std::vector<std::string> columns = {"base", "qual"};

  const std::string& username = ceph_config_json["client"];
  const std::string& cluster_name = ceph_config_json["cluster"];
  const std::string& name_space = ceph_config_json["namespace"];
  const std::string& ceph_conf_file = ceph_config_json["conf_file"];

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDCephReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDCephReader::Create(
      columns, cluster_name, username, name_space, ceph_conf_file,
      input_queue, 4, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  std::unique_ptr<ParallelAligner> aligner;

  ERR_RETURN_IF_ERROR(ParallelAligner::Create(/*threads*/ 1, index, options,
                                              chunk_queue, filter_contig_index, aligner));

  auto aln_queue = aligner->GetOutputQueue();

  // TODO convert to AGDCephWriter
  std::unique_ptr<agd::AGDFileSystemWriter> writer;
  ERR_RETURN_IF_ERROR(agd::AGDFileSystemWriter::Create({"aln"}, aln_queue, 10,
                                                       buf_pool, writer));

  if (max_records > 0) {
    while (writer->GetNumWritten() != max_records) {
      std::this_thread::sleep_for(500ms);
    }
  } else {
    // else run forever 
    // TODO respond to stop signals
    while (true) {
      std::this_thread::sleep_for(500ms);
    }
  }

  reader->Stop();
  aligner->Stop();
  writer->Stop();

  return Status::OK();
}
