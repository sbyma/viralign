#include "args.h"
#include "sample_separator.h"
#include "fastq_parser.h"
#include "libagd/src/filemap.h"
#include <stdint.h>


using namespace std;
using namespace errors;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  args::ArgumentParser parser("samplesep", "Separate samples from paired FASTQ.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> outdir_arg(
      parser, "output dir",
      "Output directory for the AGD dataset [new dir named `name_arg`]",
      {'o', "output_dir"});
  args::ValueFlag<unsigned int> chunk_size_arg(parser, "chunksize",
                                               "AGD output chunk size [100000]",
                                               {'c', "chunksize"});
  args::PositionalList<std::string> fastq_files(
      parser, "data and sample",
      "Sample first, then reads");

  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help) {
    std::cout << parser;
    return 0;
  } catch (args::ParseError e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  auto& fastq_files_vec = args::get(fastq_files);
  auto chunk_size = args::get(chunk_size_arg);
  std::cout << "Using chunk size: " << chunk_size << "\n";
  auto output_dir = args::get(outdir_arg);
  Status s;

  char* sample_file_ptr, *read_file_ptr;
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
  
  SampleSeparator separator(&read_parser, &sample_parser, chunk_size, output_dir);

  s = separator.Separate();

  if (!s.ok()) {
    cout << "[samplesep] error: " << s.error_message() << "\n";
  }

  return 0;
}