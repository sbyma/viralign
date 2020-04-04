#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "libagd/src/buffer.h"
#include "libagd/src/object_pool.h"
#include "liberr/errors.h"

using namespace errors;

// essentially just a view on a portion of a fastq file in a region of memory
class FastqChunk {
 public:
  FastqChunk(const char *start_ptr, const char *end_ptr,
             const std::size_t max_records);
  FastqChunk() = default;
  virtual ~FastqChunk() {}
  // qFastqChunk &operator=(FastqChunk &&other);

  Status GetNextRecord(const char **bases, size_t *bases_len,
                       const char **quals, const char **meta, size_t *meta_len);

  std::size_t NumRecords() const;

  bool ResetIter();

  bool IsValid() { return start_ptr_ != nullptr; }

 private:
  void read_line(const char **line_start, std::size_t *line_length,
                 std::size_t skip_length = 0);
  void skip_line();

 protected:
  const char *start_ptr_ = nullptr, *end_ptr_ = nullptr,
             *current_record_ = nullptr;
  std::size_t max_records_ = 0, current_record_idx_ = 0;

  // prevent unintended copying and assignment
  /*FastqChunk(const FastqChunk &other) = delete;
  FastqChunk &operator=(const FastqChunk &other) = delete;
  FastqResource(FastqResource &&other) = delete;*/
};

class BufferedFastqChunk : public FastqChunk {
 public:
  BufferedFastqChunk() = default;
  ~BufferedFastqChunk() {}
  BufferedFastqChunk &operator=(BufferedFastqChunk &&other) = default;
  BufferedFastqChunk(agd::ObjectPool<agd::Buffer>::ptr_type &buf,
                     const size_t max_records)
      : buf_(std::move(buf)) {
    start_ptr_ = buf_->data();
    end_ptr_ = buf_->data() + buf_->size();
    current_record_ = start_ptr_;
    max_records_ = max_records;
  }

 private:
  agd::ObjectPool<agd::Buffer>::ptr_type buf_;
};
