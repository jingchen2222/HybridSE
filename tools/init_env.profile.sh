#!/bin/bash
echo "CICD environment tag: ${CICD_RUNNER_TAG}"
echo "Third party packages path: ${CICD_RUNNER_THIRDPARTY_PATH}"
if [[ ${CICD_RUNNER_TAG} == "macos" ]]; then
    echo "Run in mac environment"
    ln -sf ${CICD_RUNNER_THIRDPARTY_PATH} thirdparty
else
    ln -sf /depends/thirdparty thirdparty
    source /opt/rh/python27/enable
    source /opt/rh/devtoolset-7/enable
    export JAVA_HOME=${PWD}/thirdparty/jdk1.8.0_141
    export PATH=${PWD}/thirdparty/bin:$JAVA_HOME/bin:${PWD}/thirdparty/apache-maven-3.6.3/bin:$PATH
fi
