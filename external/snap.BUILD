
cc_library(
    name = "snap_lib",
    #srcs = glob(["libsnap.a"]),
    srcs = glob(["snap-master/SNAPLib/*.cpp"]),
    hdrs = glob(["snap-master/SNAPLib/*.h"]),
    includes = ["snap-master/SNAPLib"],
    linkopts = [
        "-lm", 
        "-lrt", 
        "-lz"
    ],
    linkstatic = 1,
    visibility = ["//visibility:public"],
    copts = ["-pthread", "-MMD", "-msse", "-mssse3", "-msse4.2", "-Wno-format", "-std=c++11"]
)
