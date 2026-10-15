import sys,time,os,subprocess,threading
sys.argv=["x","none"]
exec(open("/root/ruvio-bench/s1_more.py").read().split("which=sys.argv[1]")[0])
DUR=int(os.environ.get("SOAK_SECS","900"))
def cpu(pid):
    f=open(f"/proc/{pid}/stat").read().rsplit(")",1)[1].split(); return (int(f[11])+int(f[12]))/os.sysconf("SC_CLK_TCK")
def rss(pid):
    for l in open(f"/proc/{pid}/status"):
        if l.startswith("VmRSS"): return int(l.split()[1])
for kind in sys.argv_kinds if hasattr(sys,"argv_kinds") else ["inline","worker"]:
    p,port,d=start(kind,False); pid=p.pid; peak=0; stopf=False
    def samp():
        global peak
        while not stopf:
            peak=max(peak,rss(pid)); time.sleep(5)
    t=threading.Thread(target=samp); t.start()
    c0=cpu(pid); t0=time.time(); total=0; errs=0
    while time.time()-t0<DUR:
        b=["taskset","-c","0","redis-benchmark","-p",str(port),"-d","64","-r","1000000","-c","200","-P","1","--csv","-n","1500000","-t","set"]
        r=subprocess.run(b,capture_output=True,text=True)
        if r.returncode!=0 or "rror" in r.stderr: errs+=1
        total+=1500000
    el=time.time()-t0; c1=cpu(pid); stopf=True; t.join()
    print(f"{kind}: {total/el:,.0f} req/s avg, {total} req in {el:.0f}s, cpu {(c1-c0)/total*1e6:.2f} us/req, rss peak {peak/1024:.0f} MiB, bench errors {errs}",flush=True)
    stop(p,d)
