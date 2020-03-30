
#include "agd_writer.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include "absl/strings/str_cat.h"
#include "libagd/src/format.h"

using namespace std;

void CreateIfNotExist(const std::string& output_dir) {
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

Status AGDWriter::Init(const std::string& output_dir) {
  CreateIfNotExist(output_dir);
  output_dir_ = output_dir;
  return Status::OK();
}

Status AGDWriter::Write(const OutputQueueItem& item) {
  // combine with header and write chunk files
  agd::format::FileHeader header;

  header.record_type = agd::format::RecordType::TEXT;
  header.compression_type = agd::format::CompressionType::GZIP;

  memset(header.string_id, 0, sizeof(agd::format::FileHeader::string_id));
  auto copy_size =
      min(dataset_name_.size(), sizeof(agd::format::FileHeader::string_id));
  strncpy(&header.string_id[0], dataset_name_.c_str(), copy_size);

  header.first_ordinal = item.first_ordinal;
  header.last_ordinal = item.first_ordinal + item.columns.chunk_size;

  auto bases_name = absl::StrCat(output_dir_, dataset_name_, "_",
                                 header.first_ordinal, ".base");
  auto qual_name = absl::StrCat(output_dir_, dataset_name_, "_",
                                header.first_ordinal, ".qual");
  auto meta_name = absl::StrCat(output_dir_, dataset_name_, "_",
                                header.first_ordinal, ".meta");

  std::ofstream bases_file(bases_name, std::ios::binary);
  bases_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
  bases_file.write(item.columns.base->data(), item.columns.base->size());

  if (!bases_file.good()) {
    return errors::Internal("Failed to write bases file ", bases_name);
  }
  bases_file.close();

  std::ofstream qual_file(qual_name, std::ios::binary);
  qual_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
  qual_file.write(item.columns.qual->data(), item.columns.qual->size());

  if (!qual_file.good()) {
    return errors::Internal("Failed to write qual file ", qual_name);
  }
  qual_file.close();

  std::ofstream meta_file(meta_name, std::ios::binary);
  meta_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
  meta_file.write(item.columns.meta->data(), item.columns.meta->size());

  if (!meta_file.good()) {
    return errors::Internal("Failed to write meta file ", meta_name);
  }
  meta_file.close();

  nlohmann::json j;
  j["first"] = item.first_ordinal;
  j["last"] = item.first_ordinal + item.columns.chunk_size;
  j["path"] = absl::StrCat(dataset_name_, "_", header.first_ordinal);

  records_.push_back(j);

  return Status::OK();
}