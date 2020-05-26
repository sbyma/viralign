cc_binary(
    name = "samplesep",
    srcs = glob([
        "src/*.cc",
        "src/*.h",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//libagd",
        "//liberr",
        "@args",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)
