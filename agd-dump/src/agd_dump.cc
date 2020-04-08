
#include <sys/stat.h>
#include <sys/types.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "args.h"
#include "libagd/src/agd_dataset.h"
#include "libagd/src/proto/alignment.pb.h"
#include "liberr/errors.h"

using namespace std;
using namespace errors;
using namespace std::chrono_literals;
using json = nlohmann::json;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    cout << "Error: " << s.error_message() << "\n";
    exit(0);
  }
}

void DumpValue(absl::string_view column, const char* data, size_t data_len) {
  if (column == "base" || column == "qual" || column == "meta") {
    cout << string(data, data_len) << "\n";
  } else if (column == "aln") {
    Alignment aln;
    bool parsed = aln.ParseFromArray(data, data_len);
    if (!parsed) cout << "failed to parse alignment\n";
    cout << "alignment: ";
    cout << aln.ShortDebugString() << "\n";
  }
}

int main(int argc, char** argv) {
  args::ArgumentParser parser("agd-dump", "Output AGD records.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::Positional<std::string> dataset_arg(
      parser, "dataset path",
      "AGD dataset to output. Specifiy the metadata.json file.");
  args::Positional<uint32_t> index_lower_arg(parser, "index low",
                                             "Lower index span to output.");
  args::Positional<uint32_t> index_upper_arg(parser, "index upper",
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
  if (path.substr(pos + 1, path.size()) != "json") {
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

  ifstream i(path);
  json agd_metadata;
  i >> agd_metadata;

  auto& columns = agd_metadata["columns"];

  std::vector<agd::AGDBufferedDataset::ColumnIterator> column_iters(
      columns.size());

  size_t col_idx = 0;
  for (const auto& col : columns) {
    std::cout << "[agddump] loading column: " << col << "\n";
    s = dataset->Column(col, &column_iters[col_idx]);
    CheckStatus(s);
    col_idx++;
  }

  const char* data;
  size_t data_len;

  col_idx = 0;
  for (const auto& col : columns) {
    s = column_iters[col_idx].GetRecordAt(idx_low, &data, &data_len);
    CheckStatus(s);
    DumpValue(col, data, data_len);
    col_idx++;
  }
  cout << "\n";

  col_idx = 0;
  idx_low++;
  for (; idx_low < idx_up; idx_low++) {
    for (const auto& col : columns) {
      s = column_iters[col_idx].GetNextRecord(&data, &data_len);
      CheckStatus(s);
      DumpValue(col, data, data_len);
      col_idx++;
    }
    cout << "\n";
    col_idx = 0;
  }

  return 0;
}