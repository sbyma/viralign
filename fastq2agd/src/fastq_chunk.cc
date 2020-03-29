#include "fastq_chunk.h"
#include <utility>


using namespace std;

/*FastqChunk& FastqChunk::operator=(FastqChunk&& other) {
  start_ptr_ = other.start_ptr_;
  other.start_ptr_ = nullptr;
  end_ptr_ = other.end_ptr_;
  other.end_ptr_ = nullptr;
  current_record_ = other.current_record_;
  other.current_record_ = nullptr;
  max_records_ = other.max_records_;
  other.max_records_ = 0;
  current_record_idx_ = other.current_record_idx_;
  other.current_record_idx_ = 0;
  return *this;
}*/

FastqChunk::FastqChunk(const char* start_ptr, const char* end_ptr,
           const std::size_t max_records)
    : start_ptr_(start_ptr),
      end_ptr_(end_ptr),
      current_record_(start_ptr),
      max_records_(max_records) {}

Status FastqChunk::GetNextRecord(const char** bases, size_t* bases_len,
                                   const char** quals, const char** meta,
                                   size_t* meta_len) {
  if (current_record_idx_ == max_records_) {
    return ResourceExhausted("no more records in this file");
  }

  read_line(meta, meta_len, 1);  // +1 to skip '@'
  read_line(bases, bases_len);
  skip_line();
  read_line(quals, bases_len);
  current_record_idx_++;

  return Status::OK();
}

void FastqChunk::read_line(const char** line_start, size_t* line_length,
                           size_t skip_length) {
  current_record_ += skip_length;
  *line_start = current_record_;
  size_t record_size = 0;

  for (; *current_record_ != '\n' && current_record_ < end_ptr_;
       record_size++, current_record_++)
    ;

  *line_length = record_size;
  current_record_++;  // to skip over the '\n'
}

void FastqChunk::skip_line() {
  for (; *current_record_ != '\n' && current_record_ < end_ptr_;
       current_record_++)
    ;
  /* include this check if we want to avoid leaving the pointers in an invalid
  state currently they won't point to anything, so it should be fine if
  (current_record_ < end_ptr_) { current_record_++;
  }
  */
  current_record_++;  // to skip over the '\n'
}

bool FastqChunk::ResetIter() {
  current_record_ = start_ptr_;
  current_record_idx_ = 0;
  return true;
}

size_t FastqChunk::NumRecords() const { return max_records_; }

