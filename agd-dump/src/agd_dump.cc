
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>
#include "absl/strings/str_cat.h"
#include "args.h"
#include "liberr/errors.h"
#include "libagd/src/agd_dataset.h"

using namespace std;
using namespace errors;
using namespace std::chrono_literals;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    cout << "Error: " << s.error_message() << "\n";
    exit(0);
  }
}

int main(int argc, char** argv) {
  args::ArgumentParser parser("agd-dump", "Output AGD records.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::Positional<std::string> dataset_arg(parser, "dataset path", "AGD dataset to output. Specifiy the metadata.json file.");
  args::Positional<uint32_t> index_lower_arg(
      parser, "index low",
      "Lower index span to output.");
  args::Positional<uint32_t> index_upper_arg(
      parser, "index upper",
      "Upper index span to output.");

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

  const auto& path = args::get(dataset_arg);
  int pos = path.find_last_of(".");
  if (path.substr(pos+1, path.size()) != "json") {
    cout << "AGD metadata file has incorect format, should be json.\n";
    return 0;
  }

  auto idx_low = args::get(index_lower_arg);
  auto idx_up = args::get(index_upper_arg);
  if (idx_low > idx_up) {
    cout << "Idx low must be <= idx up\n";
    return 0;
  }

  std::unique_ptr<agd::AGDBufferedDataset> dataset;
  auto s = agd::AGDBufferedDataset::Create(path, dataset);
  if (!s.ok()) {
    cout << "Error opening AGD dataset: " << s.error_message() << "\n";
    return 0;
  }

  agd::AGDBufferedDataset::ColumnIterator c_base, c_qual, c_meta;

  s = dataset->Column("base", &c_base);
  CheckStatus(s);

  s = dataset->Column("qual", &c_qual);
  CheckStatus(s);

  s = dataset->Column("meta", &c_meta);
  CheckStatus(s);

  const char* base, *qual, *meta;
  size_t base_len, qual_len, meta_len;

  s = c_base.GetRecordAt(idx_low, &base, &base_len);
  CheckStatus(s);
  s = c_qual.GetRecordAt(idx_low, &qual, &qual_len);
  CheckStatus(s);
  s = c_meta.GetRecordAt(idx_low, &meta, &meta_len);
  CheckStatus(s);

  cout << string(meta, meta_len) << "\n";
  cout << string(base, base_len) << "\n";
  cout << string(qual, qual_len) << "\n\n";

  idx_low++;
  for (; idx_low < idx_up; idx_low++) {
    s = c_base.GetNextRecord(&base, &base_len);
    CheckStatus(s);
    s = c_qual.GetNextRecord(&qual, &qual_len);
    CheckStatus(s);
    s = c_meta.GetNextRecord(&meta, &meta_len);
    CheckStatus(s);

    cout << string(meta, meta_len) << "\n";
    cout << string(base, base_len) << "\n";
    cout << string(qual, qual_len) << "\n\n";
  }

  return 0;
}