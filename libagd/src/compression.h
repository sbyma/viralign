#pragma once

#include "buffer.h"
#include "liberr/errors.h"

#include <zlib.h>
#include <vector>

namespace agd {

using namespace errors;

Status decompressGZIP(const char *segment, const std::size_t segment_size,
                      std::vector<char> &output);

Status decompressGZIP(const char *segment, const std::size_t segment_size,
                      Buffer *output);

Status compressGZIP(const char *segment, const std::size_t segment_size,
                    std::vector<char> &output);

class AppendingGZIPCompressor {
 public:
  AppendingGZIPCompressor(Buffer &output);

  ~AppendingGZIPCompressor();

  // reinitializes the stream
  Status init();

  Status appendGZIP(const char *segment, const std::size_t segment_size);

  // closes the stream
  Status finish();  // somehow flush

 private:
  z_stream stream_ = {0};
  bool done_ = false;
  Buffer &output_;

  void ensure_extend_capacity(std::size_t capacity);
};

}  // namespace agd
