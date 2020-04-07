#pragma once

#include <rados/librados.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "buffer.h"
#include "concurrent_queue/concurrent_queue.h"
#include "format.h"
#include "liberr/errors.h"
#include "object_pool.h"
#include "queue_defs.h"

using namespace errors;

namespace agd {

// Writes chunks to ceph.
class AGDCephWriter {
 public:
  using InputQueueItem = agd::WriteQueueItem;
  using InputQueueType = agd::WriteQueueType;

  static Status Create(std::vector<std::string> columns,
                       const std::string& cluster_name,
                       const std::string& user_name,
                       const std::string& name_space,
                       const std::string& ceph_conf_file,
                       InputQueueType* input_queue,
                       size_t threads,
                       ObjectPool<Buffer>& buf_pool,
                       std::unique_ptr<AGDCephWriter>& writer);

  uint32_t GetNumWritten() { return num_written_.load(); };

  void Stop();

 private:
  AGDCephWriter() = delete;
  AGDCephWriter(std::vector<std::string>& columns,
                ObjectPool<Buffer>& buf_pool,
                InputQueueType* input_queue)
      : columns_(columns), buf_pool_(&buf_pool), input_queue_(input_queue) {}

  Status Initialize(const std::string& cluster_name,
                    const std::string& user_name,
                    const std::string& name_space,
                    const std::string& ceph_conf_file,
                    size_t threads);

  Status setup_ceph_connection(const std::string& cluster_name,
                               const std::string& user_name,
                               const std::string& ceph_conf_file);
  void create_io_ctx(const InputQueueItem& item,
                     const std::string& name_space,
                     librados::IoCtx* io_ctx);
  ceph::bufferlist write_file(const std::string& objId,
                              librados::IoCtx& io_ctx);

  struct FormatValue {
    format::RecordType type;
    format::CompressionType compress_type;
  };

  absl::flat_hash_map<absl::string_view, FormatValue> column_map_;

  std::vector<std::string> columns_;
  ObjectPool<Buffer>* buf_pool_;  // does not own

  InputQueueType* input_queue_;

  librados::Rados cluster_;

  volatile bool done_ = false;

  std::vector<std::thread> compress_and_write_threads_;
  std::atomic_uint32_t num_written_{0};
};

}  // namespace agd
