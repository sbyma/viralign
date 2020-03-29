#include "fastq_chunker.h"

using namespace std;

// note: copies the shared ptr and any custom deleter (which we'll use)
FastqChunker::FastqChunker(const char *data, const uint64_t file_size,
                           const size_t chunk_size)
    : data_(data), chunk_size_(chunk_size) {
  current_ptr_ = data;
  end_ptr_ = current_ptr_ + file_size;
}

bool FastqChunker::next_chunk(FastqChunk &chunk) {
  const char *record_base = current_ptr_;
  size_t record_count = 0;
  while (record_count < chunk_size_ && advance_record()) {
    record_count++;
  }

  // happens if the underlying pointer arithmetic detectse that this is already
  // exhausted
  if (record_count == 0) {
    return false;
  }

  // create a fastq resource
  chunk = FastqChunk(record_base, current_ptr_, record_count);

  return true;
}

// just assume the basic 4-line format for now
bool FastqChunker::advance_record() {
  for (int i = 0; i < 4; ++i) {
    if (!advance_line()) {
      return false;
    }
  }
  return true;
}

bool FastqChunker::advance_line() {
  if (current_ptr_ == end_ptr_) {
    return false;
  } else {
    while (current_ptr_ < end_ptr_ && *current_ptr_ != '\n') {
      current_ptr_++;  // yes, you can put this in the 2nd clause expression,
                       // but that is confusing
    }

    // in this case, we want to advance OVER the '\n', as that is what caused us
    // to exit the loop
    if (current_ptr_ < end_ptr_) {
      current_ptr_++;
    }
    return true;
  }
}
