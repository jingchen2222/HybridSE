#! /bin/sh
#
# compile_in_docker.sh

ln -sf /depends/thirdparty thirdparty
source /opt/rh/devtoolset-8/enable
PWD=`pwd`
export PATH=${PWD}/thirdparty/bin:$PATH
