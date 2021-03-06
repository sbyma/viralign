#include <cstdlib>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "libagd/src/agd_ceph_reader.h"

int main(int argc, char** argv) {

  std::vector<std::string> columns = {"base", "qual", "meta"};
  const std::string cluster_name = "ragnar";
  const std::string user_name = "client.lauzhack";
  const std::string name_space = "lauzhack";
  const std::string ceph_conf_file = "/scratch/ceph_conf/lauzhack.conf";
  std::unique_ptr<agd::AGDCephReader::InputQueueType> input_queue =
    std::make_unique<agd::AGDCephReader::InputQueueType>(5);
  size_t threads = 1;
  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDCephReader> reader;

  agd::AGDCephReader::InputQueueItem inputItem;
  inputItem.objName = "testdataset_0";
  inputItem.pool = "lauzhack";
  input_queue.get()->push(inputItem);

  auto s = agd::AGDCephReader::Create(columns, cluster_name, user_name, name_space,
                                      ceph_conf_file, input_queue.get(),
                                      threads, buf_pool, reader);

  if (!s.ok()) {
    std::cerr << "[ceph_reader] AGDCephReader reader creation failed: "
              << s.error_message() << std::endl;
    return EXIT_FAILURE;
  }

  auto chunk_queue = reader->GetOutputQueue();
  reader->Stop();

  return EXIT_SUCCESS;
}
