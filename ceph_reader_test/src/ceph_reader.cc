#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "libagd/src/agd_ceph_reader.h"

int main(int argc, char** argv) {

  std::vector<std::string> columns = {"base", "qual"};
  const std::string& cluster_name = "ragnar";
  const std::string& user_name = "client.lauzhack";
  const std::string& ceph_conf_file = "/scratch/ceph_conf/lauzhack.conf";
  std::unique_ptr<agd::AGDCephReader::InputQueueType> input_queue =
    std::make_unique<agd::AGDCephReader::InputQueueType>(5);
  size_t threads = 4;
  agd::ObjectPool<agd::Buffer> buf_pool;
  std::unique_ptr<agd::AGDCephReader> reader; // TODO: why does this cause destructor of librados to be called?

  // auto reader = agd::AGDCephReader::Create(columns, )

  return EXIT_SUCCESS;
}
