#!/usr/bin/env python3
"""Server-side hardware counters per request, Ruvio vs Redis (Linux, perf). Pinned: server cpu 1, client cpu 0.

    PERF_OUT=perf.json ONLY=GET,SET python3 scripts/perf_compare.py target/release/ruvio   (needs redis-server and redis-benchmark on PATH)
"""
import subprocess, sys, time, tempfile, os, re, json, socket
RUVIO = sys.argv[1]
ROWS = [
    ("GET", ["GET", "k"]), ("SET", ["SET", "k", "x"]), ("INCR", ["INCR", "ctr"]),
    ("HSET", ["HSET", "h", "f1", "v1"]), ("HGET", ["HGET", "h", "f1"]),
    ("LPUSH+LPOP mix: LPUSH", ["LPUSH", "lp", "x"]), ("LLEN", ["LLEN", "lst2"]),
    ("EXPIRE", ["EXPIRE", "ttlk", "100000"]), ("EXISTS", ["EXISTS", "k"]), ("TTL", ["TTL", "ttlk"]),
    ("SETEX", ["SETEX", "ex:__rand_int__", "100", "v"]), ("ZADD", ["ZADD", "z", "1", "a"]),
    ("TOUCH", ["TOUCH", "k"]), ("SETRANGE", ["SETRANGE", "k", "4", "xx"]), ("BITFIELD INCRBY", ["BITFIELD", "bf", "INCRBY", "u8", "800", "1"]),
    ("COPY", ["COPY", "k", "k2", "REPLACE"]), ("MSET", ["MSET", "ma", "1", "mb", "2", "mc", "3"]), ("PUBLISH", ["PUBLISH", "ch", "m"]),
]
SETUP = [["SET","k","x"*64],["SET","ctr","0"],["HSET","h","f1","v1","f2","v2"],["RPUSH","lst2","a","b","c"],["SET","ttlk","x","EX","100000"],
         ["ZADD","z","1","a","2","b"],["SET","bf","0"],["BITFIELD","bf","SET","u8","1000","1"]]
N = 1_000_000
def frame(*a):
    a=[x.encode() for x in a]; return b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)
def cmd(port,*a):
    s=socket.create_connection(("127.0.0.1",port)); s.sendall(frame(*a)); s.recv(4096); s.close()
def start(name,port):
    d=tempfile.mkdtemp()
    if name=="ruvio": argv=[RUVIO,"--address",f"127.0.0.1:{port}","--durability","memory","--shards","1","--data-dir",d+"/data"]
    else: argv=["redis-server","--bind","127.0.0.1","--port",str(port),"--save","","--appendonly","no","--dir",d]
    p=subprocess.Popen(["taskset","-c","1"]+argv,cwd=d,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(100):
        try: cmd(port,"PING"); break
        except OSError: time.sleep(0.1)
    return p
def run(name,port,argv):
    p=start(name,port)
    try:
        for s in SETUP: cmd(port,*s)
        bench=["taskset","-c","0","redis-benchmark","-h","127.0.0.1","-p",str(port),"-n",str(N),"-c","50","-P","16","-r","1000","-q"]+argv
        subprocess.run(bench[:-len(argv)]+["-n","200000"]+argv,capture_output=True)  # warm up
        stat=subprocess.Popen(["perf","stat","-x,","-e","cycles,instructions,L1-icache-load-misses,branch-misses","-p",str(p.pid)],stderr=subprocess.PIPE,text=True)
        t=time.time(); out=subprocess.run(bench,capture_output=True,text=True).stdout; dt=time.time()-t
        stat.send_signal(2); err=stat.communicate()[1]
        m={}
        for line in err.splitlines():
            f=line.split(",")
            if len(f)>=3 and f[0].replace(".","").isdigit(): m[f[2].split(":")[0]]=float(f[0])
        rps=float(re.findall(r"([\d.]+) requests per second",out.replace("\r","\n"))[-1]) if "requests per second" in out else 0
        return {k:v/N for k,v in m.items()}|{"rps":rps}
    finally:
        p.terminate(); p.wait()
res=[]
for label,argv in ROWS:
    argv=[a.replace("LPUSH+LPOP mix: ","") for a in argv]
    row={"label":label}
    for name,port in (("ruvio",6431),("redis",6432)):
        try: row[name]=run(name,port,argv)
        except Exception as e: row[name]={"err":str(e)}
    res.append(row)
    r,d=row["ruvio"],row["redis"]
    if "cycles" in r and "cycles" in d:
        print(f"{label:22} cyc/req ruvio {r['cycles']:7.0f} redis {d['cycles']:7.0f}  insn {r['instructions']:6.0f}/{d['instructions']:6.0f}  L1i miss {r.get('L1-icache-load-misses',0):5.2f}/{d.get('L1-icache-load-misses',0):5.2f}  br-miss {r.get('branch-misses',0):5.2f}/{d.get('branch-misses',0):5.2f}  rps {r['rps']:.0f}/{d['rps']:.0f}",flush=True)
    else: print(label,row,flush=True)
json.dump(res,open(os.environ.get("PERF_OUT","perf.json"),"w"),indent=1)
