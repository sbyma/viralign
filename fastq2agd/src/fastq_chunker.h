#include "fastq_chunk.h"

class FastqChunker {
 public:
  FastqChunker(const char* data, const uint64_t file_size, const size_t chunk_size);

  // Assigns a fastq resource to the parameter
  // bool indicates whether this operation was successful
  // false = no more chunks
  bool next_chunk(FastqChunk& chunk);

 private:
  bool create_chunk(FastqChunk& chunk);
  bool advance_record();
  bool advance_line();

  const char* skip_line();

  const char* data_;
  const char *current_ptr_, *end_ptr_;
  std::size_t chunk_size_;
};
