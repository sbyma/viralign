#include "fastq_parser.h"

FastqParser::FastqParser(const char *file, uint64_t size) : start_ptr_(file), end_ptr_(file+size), current_record_(file){}

Status FastqParser::GetNextRecord(const char** bases, size_t* bases_len,
                                   const char** quals, const char** meta,
                                   size_t* meta_len) {
  if (current_record_ >= end_ptr_) {
    return ResourceExhausted("no more records in this file");
  }

  read_line(meta, meta_len, 1);  // +1 to skip '@'
  read_line(bases, bases_len);
  skip_line();
  read_line(quals, bases_len);
  current_record_idx_++;

  return Status::OK();
}

void FastqParser::read_line(const char** line_start, size_t* line_length,
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

void FastqParser::skip_line() {
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