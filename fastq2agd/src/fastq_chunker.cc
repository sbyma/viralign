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

Status CompressedFastqChunker::Init(const std::string input_fastq) {
  in_strm_.reset(new zstr::ifstream(input_fastq));
  if (!in_strm_->good()) {
    return errors::Internal("unable to open compressed file ", input_fastq);
  }
  leftover_buf_ = buf_pool_->get();
  leftover_buf_->reset();
  leftover_buf_->reserve(1024*1024*50); // 1 MB ?
  return Status::OK();
}

bool adv_line(const char*& cur_ptr, const char* end_ptr) {
  if (cur_ptr == end_ptr) {
    return false;
  } else {
    while (cur_ptr < end_ptr && *cur_ptr != '\n') {
      cur_ptr++;  // yes, you can put this in the 2nd clause expression,
                       // but that is confusing
    }

    // in this case, we want to advance OVER the '\n', as that is what caused us
    // to exit the loop
    if (cur_ptr < end_ptr) {
      cur_ptr++;
    }
    return true;
  }
}

bool adv_record(const char*& cur_ptr, const char* end_ptr) {
  const char* orig = cur_ptr;
  for (int i = 0; i < 4; ++i) {
    if (!adv_line(cur_ptr, end_ptr)) {
      cur_ptr = orig; // leave ptr at beginning of unfinished record
      return false;
    }
  }
  return true;
}

uint64_t count_records(const char* cur, const char* end, size_t& recs, size_t max) {
  const char* orig_cur = cur;
  while (recs < max) {
    if (adv_record(cur, end)) {
      recs++;
    } else {
      break;
    }
  }
  return cur - orig_cur;
}

bool CompressedFastqChunker::next_chunk(BufferedFastqChunk& chunk) {

  // this was as annoying to write as it looks

    cout << "getting next chunk!\n";

  // count the number of entries in the buf
  // if not chunk_size, read more
  size_t recs = 0;
  uint64_t position = count_records(leftover_buf_->data(), leftover_buf_->data() + leftover_buf_->size(), recs, chunk_size_);
  while (recs < chunk_size_) {
    // extend buffer if we need
    if (leftover_buf_->capacity() - leftover_buf_->size() < 1024*1024*50)
      leftover_buf_->reserve(leftover_buf_->capacity() + 1024*1024*50); // extend by 50MB

    in_strm_->read(leftover_buf_->mutable_data() + leftover_buf_->size(), leftover_buf_->capacity() - leftover_buf_->size());
    auto bytes = in_strm_->gcount();
    if (bytes == 0 && recs == 0) {
      return false;
    } else if (bytes == 0) {
      // no more data, but we have some recs
      break;
    }else {
      leftover_buf_->resize(leftover_buf_->size() + bytes);
    }
  
    position += count_records(leftover_buf_->data() + position, leftover_buf_->data() + leftover_buf_->size(), recs, chunk_size_);
  }

  // now enough recs are in the buffer, plus possibly some extra data
  auto new_leftover = buf_pool_->get();
  new_leftover->reset();
  new_leftover->reserve(leftover_buf_->size());

  new_leftover->AppendBuffer(leftover_buf_->data() + position, leftover_buf_->size() - position);
  leftover_buf_->resize(position);
  cout << "chunk is " << leftover_buf_->size() << " bytes\n";

  //cout << "the chunk is \n" << std::string(leftover_buf_->data(), leftover_buf_->size()) << "\n\n";

  chunk = std::move(BufferedFastqChunk(leftover_buf_, recs));
  assert(leftover_buf_.get() == nullptr);
  leftover_buf_ = std::move(new_leftover);

  return true;
}