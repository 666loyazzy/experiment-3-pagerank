#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p 输出
hbase shell -n operations.hbase 2>&1 | tee 输出/hbase-shell.log
