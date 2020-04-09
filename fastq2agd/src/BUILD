cc_binary(
  name = "fastq2agd",
  srcs = glob(["*.cc", "*.h"]),
  #copts = ["-std=c++14"],
  deps = ["@com_google_absl//absl/strings",
          "@args//:args",
          "@json//:json-cpp",
          "//libagd:libagd",
          "//liberr:liberr",
          "@zstr//:zstr",
          "//concurrent_queue:concurrent_queue"
  ],
)