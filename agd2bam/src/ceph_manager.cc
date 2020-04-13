
#include "ceph_manager.h"

#include "bam_builder.h"
#include "libagd/src/agd_ceph_reader.h"

using namespace errors;

Status CephManager::Run(agd::ReadQueueType* input_queue, size_t chunks,
                        const std::string& agd_metadata_path,
                        absl::string_view ceph_config_json_path,
                        size_t threads) {
  std::ifstream i(agd_metadata_path);
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  i.open(ceph_config_json_path.data());
  json ceph_config_json;
  i >> ceph_config_json;
  i.close();

  std::vector<std::string> columns = {"base", "qual", "meta", "aln"};

  const std::string& username = ceph_config_json["client"];
  const std::string& cluster_name = ceph_config_json["cluster"];
  const std::string& name_space = ceph_config_json["namespace"];
  const std::string& ceph_conf_file = ceph_config_json["conf_file"];

  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDCephReader> reader;
  ERR_RETURN_IF_ERROR(agd::AGDCephReader::Create(
      columns, cluster_name, username, name_space, ceph_conf_file, input_queue,
      threads, buf_pool, reader));

  auto chunk_queue = reader->GetOutputQueue();

  BamBuilder bb(chunk_queue, chunks);
  ERR_RETURN_IF_ERROR(bb.Init(agd_metadata, threads));
  ERR_RETURN_IF_ERROR(bb.Run());

  reader->Stop();
  return Status::OK();
}