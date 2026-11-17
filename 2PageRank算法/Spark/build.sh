#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p target/classes
javac --release 11 -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d target/classes PageRankSpark.java
jar --create --file pagerank-spark.jar -C target/classes .
