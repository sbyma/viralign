
#include "fastq_manager.h"
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

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

Status FastqManager::CreatePairedFastqManager(
    const std::string& file_path_1, const std::string& file_path_2,
    const size_t chunk_size, QueueType* work_queue,
    unique_ptr<FastqManager>& manager) {
  char *file_ptr_1, *file_ptr_2;
  uint64_t file_size_1, file_size_2;

  ERR_RETURN_IF_ERROR(mmap_file(file_path_1, &file_ptr_1, &file_size_1));
  ERR_RETURN_IF_ERROR(mmap_file(file_path_2, &file_ptr_2, &file_size_2));

  manager.reset(new FastqManager(file_ptr_1, file_size_1, file_ptr_2,
                                 file_size_2, chunk_size, work_queue));

  return Status::OK();
}

Status FastqManager::CreateFastqManager(const std::string& file_path,
                                        const size_t chunk_size,
                                        QueueType* work_queue,
                                        unique_ptr<FastqManager>& manager) {
  char* file_ptr;
  uint64_t file_size;
  ERR_RETURN_IF_ERROR(mmap_file(file_path, &file_ptr, &file_size));

  manager.reset(new FastqManager(file_ptr, file_size, nullptr, 0, chunk_size, work_queue));

  return Status::OK();
}

FastqManager::FastqManager(char* file_data, const uint64_t file_size,
                           char* file_data_2, const uint64_t file_size_2,
                           const size_t chunk_size, QueueType* work_queue)
    : file_data_1_(file_data),
      file_size_1_(file_size),
      file_data_2_(file_data_2),
      file_size_2_(file_size_2),
      chunk_size_(chunk_size),
      work_queue_(work_queue) {}

Status FastqManager::Run() {
  if (file_data_2_ == nullptr) {
    // single end, one file
    FastqChunker chunker(file_data_1_, file_size_1_, chunk_size_);
    FastqChunk c;
    while (chunker.next_chunk(c)) {
      FastqQueueItem item;
      item.chunk_1 = c;
      item.first_ordinal = current_ordinal_;

      current_ordinal_ += chunk_size_;

      work_queue_->push(item);
      total_chunks_++;
    }
  } else {
    // paired end, two files
    FastqChunker chunker_1(file_data_1_, file_size_1_, chunk_size_);
    FastqChunker chunker_2(file_data_2_, file_size_2_, chunk_size_);
    FastqChunk c1, c2;
    while (chunker_1.next_chunk(c1)) {
      if (!chunker_2.next_chunk(c2)) {
        return errors::Internal(
            "The two fastq files have differing numbers of records.");
      }
      FastqQueueItem item;
      item.chunk_1 = c1;
      item.chunk_2 = c2;
      item.first_ordinal = current_ordinal_;
      
      current_ordinal_ += chunk_size_ * 2;

      work_queue_->push(item);
      total_chunks_++;
    }
  }
  return Status::OK();
}