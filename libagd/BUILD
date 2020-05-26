cc_library(
    name = "libagd",
    srcs = glob(["src/*.cc"]),
    hdrs = glob([
        "src/*.h",
        "src/*.hpp",
    ]),
    linkopts = [
        "-lz",
        "-lrados",
        "-lhiredis",
        "-lstdc++fs",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//concurrent_queue",
        "//libagd/src/proto:alignment_cc_proto",
        "//liberr",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@json//:json-cpp",
        "@redisplusplus",
    ],
)
