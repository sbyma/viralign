
cc_binary(
  name = "viralign-genecount",
  srcs = glob(["src/*.cc", "src/*.h"]),
  #copts = ["-std=c++14"],
  deps = ["@com_google_absl//absl/strings",
          "@com_google_absl//absl/container:flat_hash_map",
          "@com_google_absl//absl/container:flat_hash_set",
          "@args//:args",
          "@json//:json-cpp",
          "//libagd:libagd",
          "//liberr:liberr"
  ],
)