#include "buffer_pair.h"

namespace agd {

  Buffer& BufferPair::index() {
    return index_;
  }

  Buffer& BufferPair::data() {
    return data_;
  }

  void BufferPair::reset() {
    index_.reset();
    data_.reset();
  }

} // namespace tensorflow {
