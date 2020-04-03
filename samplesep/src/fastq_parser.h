

#include "liberr/errors.h"

using namespace errors;

class FastqParser {
 public:
  FastqParser(const char *file, uint64_t size);
  Status GetNextRecord(const char **bases, size_t *bases_len,
                       const char **quals, const char **meta, size_t *meta_len);

 private:
  void read_line(const char **line_start, std::size_t *line_length,
                 std::size_t skip_length = 0);
  void skip_line();

  const char *start_ptr_ = nullptr, *end_ptr_ = nullptr,
             *current_record_ = nullptr;
  std::size_t current_record_idx_ = 0;
};