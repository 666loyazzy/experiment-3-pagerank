#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
bash build.sh
mkdir -p 输出
printf 'new\n' | java -cp "hbase-lab.jar:$(hbase classpath)" HBaseLab 2>&1 | tee 输出/hbase-java.log
