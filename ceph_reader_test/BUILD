
cc_binary(
    name = "ceph_reader",
    srcs = glob(["src/*.cc", "src/*.h"]),
    copts = ["-std=c++14"],
    linkopts = ["-lrados"],
    visibility = ["//visibility:public"],
    deps = ["@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/container:flat_hash_map",
        "//liberr:liberr",
        "@args//:args",
        "//libagd:libagd",
    ]
)
