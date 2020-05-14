#include <stdint.h>
#include <sys/mman.h>
#include <fstream>
#include <sstream>
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_split.h"
#include "args.hxx"
#include "fastq_parser.h"
#include "libagd/src/filemap.h"
#include "sample_separator.h"

using namespace std;
using namespace errors;
using namespace std::chrono_literals;

Status ParseBarcodeFile(const std::string &barcode_file_path,
                        SampleSeparator::BarcodeMap &barcode_map) {
  ifstream barcode_file(barcode_file_path);
  if (!barcode_file.is_open()) {
    return errors::Internal("Could not open file ", barcode_file_path);
  }

  string line;
  getline(barcode_file, line);

  vector<string> header = absl::StrSplit(line, '\t');
  if (header[0] != "Name" || header[1] != "B1") {
    return errors::Internal("Incorrect barcode file headers, were [", header[0],
                            ", ", header[1], "], should be [Name, B1]");
  }

  size_t barcode_len = 0;
  while (!barcode_file.eof()) {
    getline(barcode_file, line);

    if (line == "") break;

    vector<string> values = absl::StrSplit(line, '\t');

    const auto &barcode = values[1];
    if (barcode_len == 0) {
      barcode_len = barcode.size();
    } else if (barcode_len != barcode.size()) {
      return errors::Internal(
          "Non matching barcode size, they should all be the same size [",
          barcode.size(), "], should be [", barcode_len, "]");
    }

    barcode_map[barcode] = values[0];  // map barcode to sample name
  }

  cout << "[samplesep] Config file contained " << barcode_map.size()
       << " barcodes [";
  for (const auto &kv : barcode_map) {
    cout << kv.first << " ";
  }
  cout << "]\n";

  return Status::OK();
}

int main(int argc, char **argv) {
  args::ArgumentParser parser("samplesep",
                              "Separate samples from paired FASTQ.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> outdir_arg(
      parser, "output dir",
      "Output directory for the AGD dataset [new dir named `name_arg`]",
      {'o', "output_dir"});
  args::ValueFlag<std::string> config_file_arg(
      parser, "barcode config file",
      "Barcode config file as from BRBSeqTools, use only two column with "
      "headers [Name, B1]",
      {'b', "barcode_conf"});
  args::ValueFlag<unsigned int> chunk_size_arg(parser, "chunksize",
                                               "AGD output chunk size [100000]",
                                               {'c', "chunksize"});
  args::PositionalList<std::string> fastq_files(parser, "data and sample",
                                                "Sample/barcode first, then reads");

  try {
    parser.ParseCLI(argc, argv);
  } catch (const args::Completion &e) {
    std::cout << e.what();
    return 0;
  } catch (const args::Help &) {
    std::cout << parser;
    return 0;
  } catch (const args::ParseError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  SampleSeparator::BarcodeMap barcode_map;

  std::string barcode_conf_file_path("lib_example_barcodes.txt");
  if (config_file_arg) {
    barcode_conf_file_path = args::get(config_file_arg);
  }

  Status s;
  s = ParseBarcodeFile(barcode_conf_file_path, barcode_map);
  if (!s.ok()) {
    cout << "[samplesep] Error: " << s.error_message() << "\n";
    exit(0);
  }

  uint32_t barcode_len = barcode_map.begin()->first.size();

  auto &fastq_files_vec = args::get(fastq_files);
  auto chunk_size = args::get(chunk_size_arg);
  std::cout << "[samplesep] Using chunk size: " << chunk_size << "\n";
  auto output_dir = args::get(outdir_arg);

  char *sample_file_ptr, *read_file_ptr;
  uint64_t sample_file_len, read_file_len;

  s = mmap_file(fastq_files_vec[0].c_str(), &sample_file_ptr, &sample_file_len);
  if (!s.ok()) {
    std::cout << s.error_message() << "\n";
    exit(0);
  }

  s = mmap_file(fastq_files_vec[1].c_str(), &read_file_ptr, &read_file_len);
  if (!s.ok()) {
    std::cout << s.error_message() << "\n";
    exit(0);
  }

  FastqParser sample_parser(sample_file_ptr, sample_file_len);
  FastqParser read_parser(read_file_ptr, read_file_len);

  SampleSeparator::BarcodeIndices indices = std::make_pair(0, barcode_len);
  SampleSeparator separator(&read_parser, &sample_parser, chunk_size,
                            output_dir, indices);

  s = separator.Separate(barcode_map);

  munmap(sample_file_ptr, sample_file_len);
  munmap(read_file_ptr, read_file_len);

  if (!s.ok()) {
    cout << "[samplesep] error: " << s.error_message() << "\n";
  }

  return 0;
}