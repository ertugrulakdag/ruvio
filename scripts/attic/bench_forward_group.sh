cd ~
run() { # label binary shards cpus worker
  d=$(mktemp -d -p /mnt/data)
  (cd $d; RUVIO_ADDR=127.0.0.1:6831 RUVIO_SHARDS=$3 RUVIO_DURABILITY=always RUVIO_DATA_DIR=$d/data RUVIO_CPUSET=$4 RUVIO_SYNC_WORKER=$5 taskset -c $4 $2 >/dev/null 2>&1 &)
  sleep 1.5
  r=$(taskset -c 4,5,6,7 redis-benchmark -p 6831 -t set -n 100000 -c 50 -d 64 -r 100000 --threads 2 --csv 2>/dev/null | tail -1 | cut -d, -f2,6,7)
  syncs=0; cmds=0
  for i in 1 2 3 4 5 6 7 8; do redis-cli -p 6831 INFO prometheus | grep -E "^ruvio_wal_syncs_total" ; done | sort -u > /tmp/sy.txt
  syncs=$(awk '{s+=$NF} END {print s}' /tmp/sy.txt)
  echo "$1 shards=$3 worker=$5: rps,p50,p99=$r  fsyncs=$syncs"
  pkill -f "[r]uvio-base"; pkill -f "[t]arget/release/ruvio"; sleep 1; rm -rf $d
}
for w in off on; do
  run base ~/ruvio-base 1 0 $w
  run base ~/ruvio-base 2 0,2 $w
  run new ~/ruvio/target/release/ruvio 2 0,2 $w
  run new ~/ruvio/target/release/ruvio 4 0,2,4,6 $w
done
