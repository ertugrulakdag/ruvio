import sys,statistics
sys.argv=["x","none"]
exec(open("/root/ruvio-bench/s1_more.py").read().split("which=sys.argv[1]")[0])
def fn(port):
    bench(port,["-t","set"],200000,50,16)   # preload ~100k keys
    return bench(port,["-t","get"],600000,50,16)
res={k:[] for k in ("inline","worker","redis")}
for r in range(7):
    order=["inline","worker","redis"]; order=order[r%3:]+order[:r%3]
    for k in order:
        p,port,d=start(k,False)
        try: res[k].append(fn(port))
        finally: stop(p,d)
    print("round",r,{k:round(v[-1][0]) for k,v in res.items()},flush=True)
for k,v in res.items(): print(k,"median %.0f/s p99 %.2fms"%(statistics.median(x[0] for x in v),statistics.median(x[1] for x in v)),"min %.0f max %.0f"%(min(x[0] for x in v),max(x[0] for x in v)))
