cc_binary(
    name = "agd-dump",
    srcs = glob([
        "src/*.cc",
        "src/*.h",
    ]),
    deps = [
        "//concurrent_queue",
        "//libagd",
        "//liberr",
        "@args",
        "@com_google_absl//absl/strings",
        "@json//:json-cpp",
    ],
)
