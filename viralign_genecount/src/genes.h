
#pragma once

#include "absl/container/flat_hash_set.h"
#include "absl/container/flat_hash_map.h"
#include <string>
#include "interval_tree.h"

struct TreeValue {
  std::string gene_id;
  std::string gene_name;
  bool strand;
};

std::ostream& operator<<(std::ostream& out, const TreeValue& t);

using GeneIntervalTree = IntervalTree<int, TreeValue>;

// map from chr (contig) to set of intervals
using IntervalMap = absl::flat_hash_map<std::string, std::vector<Interval<int, TreeValue>>>;

// map from chr (contig) to interval tree
using IntervalForest = absl::flat_hash_map<std::string, GeneIntervalTree>;

// map from gene id to gene names
using GeneIdMap = absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>;