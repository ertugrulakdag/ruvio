cd ~
run() { # shards cpus clients pipeline worker
  d=$(mktemp -d -p /mnt/data)
  (cd $d; RUVIO_ADDR=127.0.0.1:6831 RUVIO_SHARDS=$1 RUVIO_DURABILITY=always RUVIO_DATA_DIR=$d/data RUVIO_CPUSET=$2 RUVIO_SYNC_WORKER=$5 taskset -c $2 ~/ruvio/target/release/ruvio >/dev/null 2>&1 &)
  sleep 1.5
  n=200000; [ $4 = 16 ] && n=1500000
  r=$(timeout 120 taskset -c 4,5,6,7 redis-benchmark -p 6831 -t set -n $n -c $3 -P $4 -d 64 -r 1000000 --threads 4 --csv 2>/dev/null | tail -1 | cut -d, -f2,6,7)
  echo "always worker=$5 shards=$1 c=$3 P=$4: rps,p50,p99=$r"
  pkill -f "[t]arget/release/ruvio"; sleep 1; rm -rf $d
}
for w in on off; do
  for sh in "1 0" "2 0,2" "4 0,2,4,6"; do set -- $sh; run $1 $2 50 1 $w; done
  for sh in "1 0" "2 0,2" "4 0,2,4,6"; do set -- $sh; run $1 $2 100 16 $w; done
done
