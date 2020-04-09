cc_binary(
  name = "viralign-push",
  srcs = glob(["src/*.cc", "src/*.h"]),
  #copts = ["-std=c++17"],
  linkopts = ["-lstdc++fs", "-lhiredis"],
  deps = ["@com_google_absl//absl/strings",
          "@args//:args",
          "@json//:json-cpp",
          "@redisplusplus//:redisplusplus"
  ],
)