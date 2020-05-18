#include "ceph_manager.h"
#include "redis_pusher.h"

#include <chrono>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "json.hpp"
#include "libagd/src/agd_ceph_reader.h"
#include "libagd/src/agd_ceph_writer.h"
#include "snap-master/SNAPLib/Bam.h"
#include "snap-master/SNAPLib/Read.h"

using json = nlohmann::json;
using namespace std::chrono_literals;
using namespace errors;

uint32_t threads = 1;

Status CephManager::Run(const CephManagerParams& params) {

  std::ifstream ci(params.ceph_config_json_path.data());
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
      params.input_queue, params.reader_threads, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  std::unique_ptr<ParallelAligner> aligner;

  ERR_RETURN_IF_ERROR(ParallelAligner::Create(/*threads*/ params.aligner_threads, params.index, params.options,
                                              chunk_queue, params.filter_contig_index, aligner));

  auto aln_queue = aligner->GetOutputQueue();

  std::unique_ptr<agd::AGDCephWriter> writer;
  ERR_RETURN_IF_ERROR(agd::AGDCephWriter::Create(
    {"aln"}, cluster_name, username, name_space, ceph_conf_file,
    aln_queue, params.writer_threads, buf_pool, writer));

  if (params.max_records > 0) {
    while (writer->GetNumWritten() != params.max_records) {
      std::this_thread::sleep_for(500ms);
    }
  } else {
    // else run forever and 
    // TODO respond to stop signals
    RedisPusher pusher(writer->GetOutputQueue(), params.redis_addr, params.queue_name);
    pusher.Run(); // never stops
  }

  reader->Stop();
  aligner->Stop();
  writer->Stop();

  return Status::OK();
}
