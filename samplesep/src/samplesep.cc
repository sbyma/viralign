#include "args.h"
#include "sample_separator.h"
#include "fastq_parser.h"
#include <stdint.h>


using namespace std;
using namespace errors;
using namespace std::chrono_literals;

Status mmap_file(const std::string& file_path, char** file_ptr,
                 uint64_t* file_size) {
  const int fd = open(file_path.c_str(), O_RDONLY);
  struct stat st;
  if (stat(file_path.c_str(), &st) != 0) {
    return Internal("Unable to stat file ", file_path);
  }
  auto size = st.st_size;
  std::cout << "file size var is " << sizeof(size) << " bytes\n";
  char* mapped = (char*)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    return Internal("Unable to map file ", file_path, ", returned ", mapped);
  }

  *file_ptr = mapped;
  *file_size = size;
  return Status::OK();
}

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
      parser, "datasets",
      "FASTQ  dataset to convert. Provide one for single end, two for paired "
      "end. ");

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

}