
# Image based on a ubuntu
FROM ubuntu

# File Author / Maintainer 
LABEL maintainer="Stuart Byma" maintainer_email="stuart.byma@epfl.ch"

# Update the repository sources list
RUN apt-get update
RUN apt-get install -y apt-utils
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -y install tzdata

# to correctly setup ceph
RUN groupadd ceph
RUN useradd -g ceph ceph

# install bazel to build 
RUN apt install -y curl gnupg
RUN curl https://bazel.build/bazel-release.pub.gpg | apt-key add -
RUN echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN apt-get update && apt-get install -y bazel 

# Install dependencies
RUN apt-get install -y build-essential git ceph-common librados-dev libradospp-dev zlib1g-dev libhiredis-dev && apt-get clean

# make an app dir 
# build and compile viralign here however you run the container
RUN mkdir app 

# && cd app && git clone https://github.com/sbyma/viralign.git && cd viralign && bazel build -c opt --copt=-std=c++17 ...
 
