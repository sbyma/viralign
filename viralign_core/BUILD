cc_binary(
  name = "viralign-core",
  srcs = glob(["src/*.cc", "src/*.h"]),
  #copts = ["-std=c++14"],
  deps = ["@com_google_absl//absl/strings",
          "@args//:args",
          "@json//:json-cpp",
          "@snap//:snap_lib",
          "//libagd:libagd",
          "//liberr:liberr",
          "//concurrent_queue:concurrent_queue"
  ],
)