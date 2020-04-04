
#include "liberr/errors.h"

errors::Status mmap_file(const std::string& file_path, char** file_ptr,
                 uint64_t* file_size);

void unmap_file(char* file, uint64_t size);