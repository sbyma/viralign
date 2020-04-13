cc_binary(
  name = "agd2bam",
  srcs = glob(["src/*.cc", "src/*.h"]),
  #copts = ["-std=c++14"],
  deps = ["@com_google_absl//absl/strings",
          "@args//:args",
          "@json//:json-cpp",
          "//libagd:libagd",
          "//liberr:liberr",
          "//concurrent_queue:concurrent_queue",
          "@snap//:snap_lib",
  ],
)