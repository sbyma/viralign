
load("@rules_cc//cc:defs.bzl", "cc_proto_library")

cc_proto_library(
    name = "alignment_cc_proto",
    deps = [":alignment_proto"],
    visibility = ["//visibility:public"]
)

proto_library(
    name = "alignment_proto",
    srcs = ["alignment.proto"],
    deps = [
        # Well known protos should be included as deps in the
        # proto_library rules of the source files importing them.
        # A list of all @com_google_protobuf well known protos can
        # seen with:
        # `bazel query 'kind(proto_library, @com_google_protobuf//:all)'`
        "@com_google_protobuf//:any_proto",
    ],
)