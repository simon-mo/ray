#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x


MEMORY_SIZE="8G"
SHM_SIZE="4G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output

[[ ! -z "$RUN_TUNE_TESTS" ]] && bash $ROOT_DIR/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}
[[ ! -z "$RUN_DOC_TESTS" ]] && bash $ROOT_DIR/run_doc_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}
[[ ! -z "$RUN_SGD_TESTS" ]] && bash $ROOT_DIR/run_sgd_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}