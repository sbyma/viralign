
#include "dataset_writer.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>

#include "compression.h"

namespace agd {

using namespace std;

void CreateDirIfNotExist(const std::string& output_dir) {
  struct stat info;
  if (stat(output_dir.c_str(), &info) != 0) {
    // doesnt exist, create
    cout << "creating dir " << output_dir << "\n";
    int e = mkdir(output_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (e != 0) {
      cout << "could not create output dir " << output_dir << ", exiting ...\n";
      exit(0);
    }
  } else if (!(info.st_mode & S_IFDIR)) {
    // exists but not dir
    cout << "output dir exists but is not dir, exiting ...\n";
    exit(0);
  } else {
    // dir exists, nuke
    // im too lazy to do this the proper way
    string cmd = absl::StrCat("rm -rf ", output_dir, "/*");
    cout << "dir " << output_dir << " exists, nuking ...\n";
    int nuke_result = system(cmd.c_str());
    if (nuke_result != 0) {
      cout << "Could not nuke dir " << output_dir << "\n";
      exit(0);
    }
  }
}

Status DatasetWriter::CreateDatasetWriter(
    size_t compress_threads, size_t write_threads, const std::string& name,
    const std::string& path, const std::vector<std::string>& columns,
    std::unique_ptr<DatasetWriter>& writer, ObjectPool<Buffer>* buf_pool) {
  CreateDirIfNotExist(path);

  writer.reset(new DatasetWriter(path, name, columns));

  ERR_RETURN_IF_ERROR(writer->Init(compress_threads, write_threads, buf_pool));

  return Status::OK();
}

Status DatasetWriter::Init(size_t compress_threads, size_t write_threads,
                           ObjectPool<Buffer>* buf_pool) {
  // todo put this kind of stuff in the format.h file
  column_map_["base"] = {
      agd::format::RecordType::TEXT,
      agd::format::CompressionType::GZIP};  // todo store as compacted bases
  column_map_["qual"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["meta"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["aln"] = {agd::format::RecordType::STRUCTURED,
                        agd::format::CompressionType::GZIP};

  buf_pool_ = buf_pool;

  chunk_queue_ = std::make_unique<ConcurrentQueue<ChunkQueueItem>>(10);
  write_queue_ = std::make_unique<ConcurrentQueue<WriteQueueItem>>(10);

  compress_threads_.resize(compress_threads);
  for (auto& t : compress_threads_) {
    t = std::thread(&DatasetWriter::compress_func, this);
  }

  write_threads_.resize(write_threads);
  for (auto& t : write_threads_) {
    t = std::thread(&DatasetWriter::write_func, this);
  }

  return Status::OK();
}

Status DatasetWriter::WriteChunks(
    std::vector<ObjectPool<BufferPair>::ptr_type>& column_bufs,
    size_t chunk_size, uint64_t first_ordinal) {
  if (column_bufs.size() != columns_.size()) {
    return errors::InvalidArgument(
        "DatasetWriter Write must supply one chunk for each column, expected ",
        columns_.size(), " chunks, received ", column_bufs.size());
  }

  for (size_t i = 0; i < column_bufs.size(); i++) {
    ChunkQueueItem item;
    item.buf = std::move(column_bufs[i]);
    item.chunk_size = chunk_size;
    item.first_ordinal = first_ordinal;
    item.column = absl::string_view(columns_[i]);
    chunk_queue_->push(std::move(item));
  }

  nlohmann::json j;
  j["first"] = first_ordinal;
  j["last"] = first_ordinal + chunk_size;
  j["path"] = absl::StrCat(name_, "_", first_ordinal);

  records_.push_back(std::move(j));

  return Status::OK();
}

void DatasetWriter::compress_func() {
  while (!done_) {
    ChunkQueueItem item;
    if (!chunk_queue_->pop(item)) continue;

    // compress the buffer into a fresh pool buffer
    auto compress_buf = buf_pool_->get();
    compress_buf->reserve(item.buf->data().size() + item.buf->index().size());
    compress_buf->reset();
    AppendingGZIPCompressor compressor(*compress_buf.get());
    Status s = Status::OK();
    s = compressor.init();
    if (!s.ok()) {
      std::cout << "Error: couldn't init compressor\n";
      exit(0);
    }
    s = compressor.appendGZIP(item.buf->index().data(),
                              item.buf->index().size());
    if (!s.ok()) {
      std::cout << "Error: couldn't init compressor\n";
      exit(0);
    }
    s = compressor.appendGZIP(item.buf->data().data(), item.buf->data().size());
    if (!s.ok()) {
      std::cout << "Error: couldn't init compressor\n";
      exit(0);
    }
    WriteQueueItem write_item;
    write_item.buf = std::move(compress_buf);
    write_item.chunk_size = item.chunk_size;
    write_item.column = item.column;
    write_item.first_ordinal = item.first_ordinal;
    write_queue_->push(std::move(write_item));
  }
}

void DatasetWriter::write_func() {
  while (!write_done_) {
    WriteQueueItem item;
    if (!write_queue_->pop(item)) continue;

    agd::format::FileHeader header;

    const auto& types = column_map_[item.column];
    header.record_type = types.type;
    header.compression_type = types.compress_type;

    memset(header.string_id, 0, sizeof(agd::format::FileHeader::string_id));
    auto copy_size =
        std::min(name_.size(), sizeof(agd::format::FileHeader::string_id));
    strncpy(&header.string_id[0], name_.c_str(), copy_size);

    header.first_ordinal = item.first_ordinal;
    header.last_ordinal = item.first_ordinal + item.chunk_size;

    auto file_name =
        absl::StrCat(path_, name_, "_", header.first_ordinal, item.column);

    std::ofstream out_file(file_name, std::ios::binary);
    out_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    out_file.write(item.buf->data(), item.buf->size());

    if (!out_file.good()) {
      cout << "Failed to write bases file " << file_name << "\n";
    }
    out_file.close();
  }
}

void DatasetWriter::Stop() {
  std::cout << "stopping dataset writer\n";
  while (!chunk_queue_->empty()) {
    std::this_thread::sleep_for(1ms);
  }
  chunk_queue_->unblock();
  done_ = true;
  for (auto& t : compress_threads_) {
    t.join();
  }
  while (!write_queue_->empty()) {
    std::this_thread::sleep_for(1ms);
  }

  write_queue_->unblock();
  write_done_ = true;
  for (auto& t : write_threads_) {
    t.join();
  }
  std::cout << "done stopping\n";
}

Status DatasetWriter::WriteMetadata() {
  nlohmann::json metadata_json;

  metadata_json["columns"] = {"base", "qual", "meta"};
  metadata_json["version"] = 1;
  metadata_json["name"] = name_;
  metadata_json["records"] = records_;

  std::ofstream o(absl::StrCat(path_, "metadata.json"));
  o << std::setw(4) << metadata_json << std::endl;

  return Status::OK();
}

}  // namespace agd