cc_binary(
  name = "viralign-push",
  srcs = glob(["src/*.cc", "src/*.h"]),
  copts = ["-std=c++14"],
  linkopts = ["-lev", "-lhiredis"],
  deps = ["@com_google_absl//absl/strings",
          "@args//:args",
          "@json//:json-cpp",
          "@redox//:redox",
          "//libagd:libagd",
          "//liberr:liberr"
  ],
)