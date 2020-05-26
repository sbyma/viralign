cc_binary(
    name = "viralign-genecount",
    srcs = glob([
        "src/*.cc",
        "src/*.h",
    ]),
    deps = [
        "//libagd",
        "//liberr",
        "@args",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@json//:json-cpp",
    ],
)
