#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
bash build.sh
run_id="$(date +%Y%m%d-%H%M%S)-$$"
base="/user/$(id -un)/pagerank-mr/$run_id"
mkdir -p 输出
hdfs dfs -mkdir -p "$base/input"
hdfs dfs -put ../数据/links.txt "$base/input/"
hadoop jar pagerank-mr.jar PageRankMR "$base/input" "$base/output" 8 2>&1 | tee "输出/job-$run_id.log"
hdfs dfs -getmerge "$base/output/iteration-10/part-r-*" 输出/result.txt
printf '%s\n' "$base" > 输出/hdfs-path.txt
