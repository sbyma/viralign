load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "com_google_absl",
    sha256 = "7f9dffeaa566b9688600cdedaff85700bd0c4dbf8373a2764add466efd165b8d",
    strip_prefix = "abseil-cpp-20200225.1",
    url = "https://github.com/abseil/abseil-cpp/archive/20200225.1.zip",
)

# rules_cc defines rules for generating C++ code from Protocol Buffers.
http_archive(
    name = "rules_cc",
    sha256 = "35f2fb4ea0b3e61ad64a369de284e4fbbdcdba71836a5555abb5e194cf119509",
    strip_prefix = "rules_cc-624b5d59dfb45672d4239422fa1e3de1822ee110",
    urls = [
        "https://github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
    ],
)

# rules_proto defines abstract rules for building Protocol Buffers.
http_archive(
    name = "rules_proto",
    sha256 = "2490dca4f249b8a9a3ab07bd1ba6eca085aaf8e45a734af92aad0c42d9dc7aaf",
    strip_prefix = "rules_proto-218ffa7dfa5408492dc86c01ee637614f8695c45",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/218ffa7dfa5408492dc86c01ee637614f8695c45.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/218ffa7dfa5408492dc86c01ee637614f8695c45.tar.gz",
    ],
)

http_archive(
    name = "snap",
    build_file = "snap.BUILD",
    sha256 = "18a5e931a053cf47f16186a0ae07c63539a39edf6d87e455011e637709e6c217",
    urls = ["https://www.dropbox.com/s/q0k6dku1hdyi3r6/snap.zip?dl=1"],
)

http_archive(
    name = "redisplusplus",
    build_file = "redispp.BUILD",
    sha256 = "17882f9253869fce31e7369ec669edc26ac4de70c1cafe711fc4e7e43fd7dd60",
    strip_prefix = "redis-plus-plus-1.1.1",
    urls = ["https://github.com/sewenew/redis-plus-plus/archive/1.1.1.zip"],
)

http_archive(
    name = "args",
    build_file = "args.BUILD",
    sha256 = "1a1c8846acd2d117843f6ab13518cac78bd0f8dcde8531603ac6f2115c9582d6",
    strip_prefix = "args-6.2.2",
    urls = ["https://github.com/Taywee/args/archive/6.2.2.zip"],
)

http_archive(
    name = "json",
    build_file = "json.BUILD",
    sha256 = "87b5884741427220d3a33df1363ae0e8b898099fbc59f1c451113f6732891014",
    strip_prefix = "single_include/nlohmann",
    urls = ["https://github.com/nlohmann/json/releases/download/v3.7.3/include.zip"],
)

load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")

rules_cc_dependencies()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()
