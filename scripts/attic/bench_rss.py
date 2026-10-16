import sys,time,os,subprocess,threading,statistics
sys.argv=["x","none"]
exec(open("/root/ruvio-bench/s1_more.py").read().split("which=sys.argv[1]")[0])
N=int(os.environ.get("RSS_N","3000000")); R=int(os.environ.get("RSS_ROUNDS","3")); C=os.environ.get("RSS_C","200")
def rss(pid):
    for l in open(f"/proc/{pid}/status"):
        if l.startswith("VmRSS"): return int(l.split()[1])
def run(kind):
    p,port,d=start(kind,False); pid=p.pid; peak=[0]; stopf=[False]
    def samp():
        while not stopf[0]: peak[0]=max(peak[0],rss(pid)); time.sleep(1)
    t=threading.Thread(target=samp); t.start()
    subprocess.run(["taskset","-c","0","redis-benchmark","-p",str(port),"-d","64","-r","1000000","-c",C,"-n",str(N),"-t","set","--csv"],capture_output=True)
    time.sleep(2); end=rss(pid); stopf[0]=True; t.join(); stop(p,d); return peak[0]/1024,end/1024
res={"inline":[],"worker":[]}
for r in range(R):
    for k in (["inline","worker"] if r%2==0 else ["worker","inline"]):
        res[k].append(run(k)); print(r,k,"peak %.0f end %.0f MiB"%res[k][-1],flush=True)
for k,v in res.items(): print(k,"median peak %.0f end %.0f"%(statistics.median(x[0] for x in v),statistics.median(x[1] for x in v)))
