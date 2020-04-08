
cc_library(
    name = "libagd",
    srcs = glob(["src/*.cc"]),
    hdrs = glob(["src/*.h", "src/*.hpp"]),
    linkopts = ["-lz", "-lrados"],
    copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
    deps = ["@com_google_absl//absl/strings",
            "@com_google_absl//absl/strings:str_format",
            "@com_google_absl//absl/synchronization",
            "@com_google_absl//absl/container:flat_hash_map",
            "@json//:json-cpp",
            "//liberr:liberr",
            "//concurrent_queue:concurrent_queue",
            "//libagd/src/proto:alignment_cc_proto"
    ]
)
