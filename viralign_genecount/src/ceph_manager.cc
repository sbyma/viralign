#include "ceph_manager.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "json.hpp"
#include "libagd/src/agd_ceph_reader.h"
#include "libagd/src/agd_ceph_writer.h"
#include "genecount.h"

using json = nlohmann::json;
using namespace std::chrono_literals;
using namespace errors;

uint32_t threads = 1;

Status CephManager::Run(const CephManagerParams& params) {
  std::ifstream ci(params.ceph_config_json_path.data());
  json ceph_config_json;
  ci >> ceph_config_json;
  ci.close();

  std::vector<std::string> columns = {"aln"};

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
  
  Status s = CountGenes(params.max_chunks, chunk_queue, params.interval_forest, *params.genes);

  reader->Stop();

  return s;
}
