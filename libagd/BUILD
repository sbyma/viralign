
cc_library(
    name = "libagd",
    srcs = glob(["src/*.cc"]),
    hdrs = glob(["src/*.h", "src/*.hpp"]),
    linkopts = ["-lz"],
    copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
    deps = ["@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@json//:json-cpp",
        "//liberr:liberr",
        "//libagd/src/proto:alignment_cc_proto"
    ]
)