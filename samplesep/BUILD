
cc_binary(
    name = "samplesep",
    srcs = glob(["src/*.cc", "src/*.h"]),
    #copts = ["-std=c++14"],
    visibility = ["//visibility:public"],
    deps = ["@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "//liberr:liberr",
        "@args//:args",
        "//libagd:libagd",
    ]
)