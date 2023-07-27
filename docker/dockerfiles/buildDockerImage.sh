#!/bin/bash
# Build docker image
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2028. All rights reserved.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# buildDockerImage.sh
#    Build docker image
#
# IDENTIFICATION
#    GaussDBKernel/server/docker/dockerfiles/buildDockerImage.sh
#
#-------------------------------------------------------------------------

usage() {
  cat << EOF

Usage: buildDockerImage.sh -v [version]  [-i]  [Docker build option]
Builds a Docker Image for openGauss
  
Parameters:
   -v: version to build
       Choose one of: $(for i in $(ls -d */); do echo -n "${i%%/}  "; done)
   -i: ignores the SHA256 checksums

LICENSE UPL 1.0


EOF

}

# Validate packages
checksum_packages() {
if [ "${arch}" = "amd64" ]; then
    sha256_file="sha256_file_amd64"
    else
    sha256_file="sha256_file_arm64"
fi

  if hash sha256sum 2>/dev/null; then
    echo "Checking if required packages are present and valid..."   
    if ! sha256sum -c "$sha256_file"; then
      echo "SHA256 for required packages to build this image did not match!"
      echo "Make sure to download missing files in folder $VERSION."
      exit 1;
    fi
  else
    echo "Ignored SHA256 sum, 'sha256sum' command not available.";
  fi
}


# Check Docker version
check_docker_version() {
  # Get Docker Server version
  echo "Checking Docker version."
  DOCKER_VERSION=$(docker version --format '{{.Server.Version | printf "%.5s" }}'|| exit 0)
  # Remove dot in Docker version
  docker_version_major=$(echo $DOCKER_VERSION | awk -F . '{print $1}')

  if [ -z "$DOCKER_VERSION" ]; then
    # docker could be aliased to podman and errored out (https://github.com/containers/libpod/pull/4608)
    echo "Please check if docker is installed." && exit 1
  elif [ "$docker_version_major" -lt "${MIN_DOCKER_VERSION_MAJOR}" ]; then
    echo "Docker version is below the minimum required version $MIN_DOCKER_VERSION_MAJOR.$MIN_DOCKER_VERSION_MINOR"
    echo "Please upgrade your Docker installation to proceed."
    exit 1;
  fi
}

##############
#### MAIN ####
##############

# Parameters
VERSION="5.0.0"
SKIPCHECKSUM=0
DOCKEROPS=""
MIN_DOCKER_VERSION_MAJOR="17"
MIN_DOCKER_VERSION_MINOR="09"
arch=$(case $(uname -m) in i386)   echo "386" ;; i686)   echo "386" ;; x86_64) echo "amd64";; aarch64)echo "arm64";; esac)
if [ "${arch}" = "amd64" ]; then
    DOCKERFILE="dockerfile_amd"
    else
    DOCKERFILE="dockerfile_arm"
fi

if [ "$#" -eq 0 ]; then
  usage;
  exit 1;
fi

while getopts "hesxiv:o:" optname; do
  case "$optname" in
    "h")
      usage
      exit 0;
      ;;
    "i")
      SKIPCHECKSUM=1
      ;;
    "v")
      VERSION="$OPTARG"
      ;;
    "o")
      DOCKEROPS="$OPTARG"
      ;;
    "?")
      usage;
      exit 1;
      ;;
    *)
    # Should not occur
      echo "Unknown error while processing options inside buildDockerImage.sh"
      ;;
  esac
done

check_docker_version


# openGauss Database Image Name
IMAGE_NAME="opengauss:$VERSION"

# Go into version folder
cd "$VERSION" || {
  echo "Could not find version directory '$VERSION'";
  exit 1;
}

if [ ! "$SKIPCHECKSUM" -eq 1 ]; then
  checksum_packages
else
  echo "Ignored SHA256 checksum."
fi
echo "=========================="
echo "DOCKER info:"
docker info
echo "=========================="

# Proxy settings
PROXY_SETTINGS=""
if [ "${http_proxy}" != "" ]; then
  PROXY_SETTINGS="$PROXY_SETTINGS --build-arg http_proxy=${http_proxy}"
fi

if [ "${https_proxy}" != "" ]; then
  PROXY_SETTINGS="$PROXY_SETTINGS --build-arg https_proxy=${https_proxy}"
fi

if [ "${ftp_proxy}" != "" ]; then
  PROXY_SETTINGS="$PROXY_SETTINGS --build-arg ftp_proxy=${ftp_proxy}"
fi

if [ "${no_proxy}" != "" ]; then
  PROXY_SETTINGS="$PROXY_SETTINGS --build-arg no_proxy=${no_proxy}"
fi

if [ "$PROXY_SETTINGS" != "" ]; then
  echo "Proxy settings were found and will be used during the build."
fi

# ################## #
# BUILDING THE IMAGE #
# ################## #
echo "Building image '$IMAGE_NAME' ..."

# BUILD THE IMAGE (replace all environment variables)
BUILD_START=$(date '+%s')
docker build --force-rm=true --no-cache=true \
       $DOCKEROPS $PROXY_SETTINGS  \
       -t $IMAGE_NAME -f $DOCKERFILE . || {
  echo ""
  echo "ERROR: openGauss Database Docker Image was NOT successfully created."
  echo "ERROR: Check the output and correct any reported problems with the docker build operation."
  exit 1
}

# Remove dangling images (intermitten images with tag <none>)
yes | docker image prune > /dev/null

BUILD_END=$(date '+%s')
BUILD_ELAPSED=$(expr $BUILD_END - $BUILD_START)

echo ""
echo ""

cat << EOF
  openGauss Docker Image  $VERSION is ready to be extended: 
    
    --> $IMAGE_NAME

  Build completed in $BUILD_ELAPSED seconds.
  
EOF
