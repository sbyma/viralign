#include "filemap.h"
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace errors;

Status mmap_file(const std::string& file_path, char** file_ptr,
                 uint64_t* file_size) {
  const int fd = open(file_path.c_str(), O_RDONLY);
  struct stat st;
  if (stat(file_path.c_str(), &st) != 0) {
    return Internal("Unable to stat file ", file_path);
  }
  auto size = st.st_size;

  char* mapped = (char*)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    return Internal("Unable to map file ", file_path, ", returned ", mapped);
  }

  *file_ptr = mapped;
  *file_size = size;
  return Status::OK();
}