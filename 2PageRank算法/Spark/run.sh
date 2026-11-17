#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
bash build.sh
run_id="$(date +%Y%m%d-%H%M%S)-$$"
base="/user/$(id -un)/pagerank-spark/$run_id"
mkdir -p 输出
hdfs dfs -mkdir -p "$base/input"
hdfs dfs -put ../数据/links.txt "$base/input/"
spark-submit --master yarn --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --num-executors 1 --conf spark.yarn.am.memory=256m --conf spark.yarn.am.memoryOverhead=128m --conf spark.executor.memoryOverhead=384 --class PageRankSpark pagerank-spark.jar "$base/input" "$base/output" 2>&1 | tee "输出/job-$run_id.log"
hdfs dfs -getmerge "$base/output/part-*" 输出/result.txt
printf '%s\n' "$base" > 输出/hdfs-path.txt
