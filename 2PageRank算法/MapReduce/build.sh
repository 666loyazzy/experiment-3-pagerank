#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p target/classes
javac --release 11 -encoding UTF-8 -cp "$(hadoop classpath --glob)" -d target/classes PageRankMR.java
jar --create --file pagerank-mr.jar -C target/classes .
