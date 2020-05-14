
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# TODO remove these local reps and use http_archive's
local_repository(
  name = "json",

  path = "third_party/json",
)

http_archive(
    name = "com_google_absl",
    url = "https://github.com/abseil/abseil-cpp/archive/20200225.1.zip",
    sha256 = "7f9dffeaa566b9688600cdedaff85700bd0c4dbf8373a2764add466efd165b8d",
    strip_prefix = "abseil-cpp-20200225.1",
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
  urls = ["https://www.dropbox.com/s/q0k6dku1hdyi3r6/snap.zip?dl=1"],
  sha256 = "18a5e931a053cf47f16186a0ae07c63539a39edf6d87e455011e637709e6c217",
  build_file = "snap.BUILD"
)

http_archive(
  name = "redisplusplus",
  urls = ["https://github.com/sewenew/redis-plus-plus/archive/1.1.1.zip"],
  sha256 = "17882f9253869fce31e7369ec669edc26ac4de70c1cafe711fc4e7e43fd7dd60",
  build_file = "redispp.BUILD",
  strip_prefix = "redis-plus-plus-1.1.1",
)

http_archive(
  name = "args",
  urls = ["https://github.com/Taywee/args/archive/6.2.2.zip"],
  sha256 = "1a1c8846acd2d117843f6ab13518cac78bd0f8dcde8531603ac6f2115c9582d6",
  build_file = "args.BUILD",
  strip_prefix = "args-6.2.2",
)

load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")
rules_cc_dependencies()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()