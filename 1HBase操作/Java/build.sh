#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p target/classes
javac --release 11 -encoding UTF-8 -cp "$(hbase classpath)" -d target/classes HBaseLab.java
jar --create --file hbase-lab.jar -C target/classes .
