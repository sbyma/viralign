
cc_binary(
    name = "samplesep",
    srcs = glob(["src/*.cc"]),
    hdrs = glob(["src/*.h", "src/*.hpp"]),
    copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
    deps = ["@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "//liberr:liberr",
        "//libagd:libagd",
    ]
)