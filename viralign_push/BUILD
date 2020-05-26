cc_binary(
    name = "viralign-push",
    srcs = glob([
        "src/*.cc",
        "src/*.h",
    ]),
    linkopts = [
        "-lstdc++fs",
        "-lhiredis",
    ],
    deps = [
        "@args",
        "@com_google_absl//absl/strings",
        "@json//:json-cpp",
        "@redisplusplus",
    ],
)
