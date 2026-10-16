// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Routes encoded commands to the node that owns their key's slot.
 *
 * <p>The slot table (16384 entries) is built from {@code CLUSTER SLOTS}. Each node has its own lazily
 * opened multiplexed {@link Link}; a lost node connection only affects commands for that node and is
 * reopened by the next command. A {@code -MOVED} reply points that one slot at the named node and the
 * command is sent there, at most {@link #MAX_REDIRECTS} times per command; the whole table is re-read from
 * the server only when the named node is unknown or from the command's second MOVED on, and the retry still
 * goes to the named node.
 */
public final class ClusterRouter implements AutoCloseable {
    public static final int SLOTS = 16384;
    /** MOVED redirects followed per command (each to the node the reply names) before the error is returned. */
    public static final int MAX_REDIRECTS = 3;

    /** Where a routed frame goes: straight onto a connection, or into a pipeline group. */
    public interface Sink {
        void send(ClusterNode node, byte[] buf, int off, int len, RoutedRequest<?> request);
    }

    /** Writes the frame to the node's shared connection right away. */
    public static final Sink DIRECT = (node, buf, off, len, request) -> {
        Connection c = node.link.get();
        RespOut o = c.begin();
        try {
            o.raw(buf, off, len);
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        c.end(request);
    };

    private static final ThreadLocal<Frame> FRAMES = ThreadLocal.withInitial(Frame::new);

    /** Runs MOVED follow-ups off the reader threads, which must never wait for a reply. */
    private static final ExecutorService BACKGROUND = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> {
                Thread t = new Thread(r, "ruvio-cluster");
                t.setDaemon(true);
                return t;
            });

    private final RuvioOptions options;
    private final ClusterNode seed;
    private final ConcurrentHashMap<String, ClusterNode> nodes = new ConcurrentHashMap<>();
    private volatile ClusterNode[] table = new ClusterNode[SLOTS];
    private final Object refreshLock = new Object();
    private volatile boolean closed;

    private ClusterRouter(RuvioOptions options, ClusterNode seed) {
        this.options = options;
        this.seed = seed;
        nodes.put(seed.address, seed);
    }

    /**
     * Asks the seed for {@code CLUSTER SLOTS}. Returns null when the server is not in cluster mode
     * (it answers with an error), in which case the caller keeps using the plain standalone path.
     */
    public static ClusterRouter detect(Link seedLink, RuvioOptions options) {
        ClusterRouter r = new ClusterRouter(options, new ClusterNode(options.host, options.port, seedLink));
        try {
            r.refresh();
        } catch (RuvioServerException e) {
            return null;
        }
        return r;
    }

    public ClusterNode seed() {
        return seed;
    }

    public long timeoutMillis() {
        return options.commandTimeoutMillis > 0 ? options.commandTimeoutMillis : 10_000;
    }

    // ---- topology --------------------------------------------------------------------------

    public ClusterNode owner(int slot) {
        ClusterNode n = table[slot];
        return n != null ? n : anyNode();
    }

    /** A node for keyless commands: the seed while its connection is up, else any live node, else the seed. */
    public ClusterNode anyNode() {
        if (seed.link.isUp()) {
            return seed;
        }
        for (ClusterNode n : nodes.values()) {
            if (n.link.isUp()) {
                return n;
            }
        }
        return seed;
    }

    /** The distinct nodes that own slots, in slot order. */
    public List<ClusterNode> masters() {
        LinkedHashSet<ClusterNode> out = new LinkedHashSet<>();
        for (ClusterNode n : table) {
            if (n != null) {
                out.add(n);
            }
        }
        return new ArrayList<>(out);
    }

    public ClusterNode node(String address) {
        return nodes.get(address);
    }

    public ClusterNode ownerOfKey(byte[] key) {
        return owner(HashSlot.of(key));
    }

    private ClusterNode addNode(String host, int port) {
        return nodes.computeIfAbsent(ClusterNode.address(host, port), a -> {
            RuvioOptions o = options.toBuilder().host(host).port(port).database(0).build();
            return new ClusterNode(host, port, new Link(o, true));
        });
    }

    /** Re-reads {@code CLUSTER SLOTS} from the first node that answers and swaps the slot table. */
    public void refresh() {
        synchronized (refreshLock) {
            RuntimeException last = null;
            List<ClusterNode> candidates = new ArrayList<>(nodes.size());
            candidates.add(seed);
            for (ClusterNode n : nodes.values()) {
                if (n != seed) {
                    candidates.add(n);
                }
            }
            for (ClusterNode n : candidates) {
                try {
                    apply(querySlots(n), n);
                    return;
                } catch (RuvioConnectionException | RuvioServerException e) {
                    last = e;
                } catch (RuvioException e) {
                    last = e; // timeout
                }
            }
            throw last != null ? last : new RuvioException("no cluster node answered CLUSTER SLOTS");
        }
    }

    private Object querySlots(ClusterNode n) {
        Connection c = n.link.get();
        Request<Object> req = new Link.Raw(c).send(Reply.RAW, "CLUSTER", "SLOTS");
        return Link.await(req, timeoutMillis());
    }

    private void apply(Object tree, ClusterNode from) {
        List<Object> ranges = Reply.list(tree);
        ClusterNode[] t = new ClusterNode[SLOTS];
        int assigned = 0;
        for (Object o : ranges) {
            List<Object> range = Reply.list(o);
            if (range.size() < 3) {
                continue;
            }
            int start = (int) (long) (Long) range.get(0);
            int end = (int) (long) (Long) range.get(1);
            List<Object> master = Reply.list(range.get(2));
            String host = Reply.str(master.get(0));
            int port = (int) (long) (Long) master.get(1);
            if (host == null || host.isEmpty()) {
                host = from.host;
            }
            ClusterNode n = nodes.get(ClusterNode.address(host, port));
            if (n == null) {
                n = addNode(host, port);
            }
            for (int s = Math.max(0, start); s <= end && s < SLOTS; s++) {
                t[s] = n;
                assigned++;
            }
        }
        if (assigned == 0) {
            throw new RuvioServerException("ERR CLUSTER SLOTS returned no slot ranges");
        }
        table = t;
    }

    // ---- routing ---------------------------------------------------------------------------

    /** The calling thread's scratch buffer for the next frame. */
    public RespOut begin() {
        Frame f = FRAMES.get();
        f.reset();
        return f.out;
    }

    /** Routes the frame in {@code f} (already encoded) and hands it to {@code sink}. */
    public void dispatch(Frame f, RoutedRequest<?> r, Sink sink) {
        f.parse();
        byte[] b = f.out.buf;
        ClusterKeys.Spec spec = ClusterKeys.spec(b, f.off[0], f.len[0]);
        int slot = -1;
        if (spec.first) {
            if (f.argc >= 2) {
                slot = HashSlot.of(b, f.off[1], f.len[1]);
            }
        } else {
            int n = ClusterKeys.keys(spec, f);
            if (n > 0) {
                int[] k = f.keyIdx;
                slot = HashSlot.of(b, f.off[k[0]], f.len[k[0]]);
                for (int i = 1; i < n; i++) {
                    if (HashSlot.of(b, f.off[k[i]], f.len[k[i]]) != slot) {
                        crossSlot(spec, f, n, r, sink);
                        return;
                    }
                }
            }
        }
        int total = f.out.pos;
        r.router = this;
        byte[] copy = Arrays.copyOf(b, total);
        r.frame = copy;
        sink.send(slot < 0 ? anyNode() : owner(slot), copy, 0, total, r);
    }

    /** The slot shared by the keys of the parsed frame; -1 when it has no key; CROSSSLOT error otherwise. */
    public static int commonSlot(Frame f) {
        f.parse();
        byte[] b = f.out.buf;
        ClusterKeys.Spec spec = ClusterKeys.spec(b, f.off[0], f.len[0]);
        int n = ClusterKeys.keys(spec, f);
        if (n == 0) {
            return -1;
        }
        int[] k = f.keyIdx;
        int slot = HashSlot.of(b, f.off[k[0]], f.len[k[0]]);
        for (int i = 1; i < n; i++) {
            if (HashSlot.of(b, f.off[k[i]], f.len[k[i]]) != slot) {
                throw crossSlotError();
            }
        }
        return slot;
    }

    public static RuvioServerException crossSlotError() {
        return new RuvioServerException("CROSSSLOT Keys in request do not hash to the same slot (checked by the client, nothing was sent)");
    }

    private void crossSlot(ClusterKeys.Spec spec, Frame f, int n, RoutedRequest<?> r, Sink sink) {
        if (spec.split == ClusterKeys.NO_SPLIT) {
            r.failFinal(crossSlotError());
            return;
        }
        split(spec, f, n, r, sink);
    }

    /**
     * MGET/MSET/DEL/UNLINK/EXISTS/TOUCH over several slots: one sub-command per slot (the server
     * requires one slot per command, even inside one shard), each sent to its owner. The sub-commands
     * run concurrently and the replies are re-assembled in the caller's order.
     */
    private void split(ClusterKeys.Spec spec, Frame f, int n, RoutedRequest<?> r, Sink sink) {
        boolean pairs = spec.pairs;
        if (pairs && (f.argc & 1) == 0) {
            r.failFinal(new RuvioServerException("ERR wrong number of arguments for '" + spec.name.toLowerCase() + "' command"));
            return;
        }
        byte[] b = f.out.buf;
        int[] k = Arrays.copyOf(f.keyIdx, n);
        Map<Integer, Integer> groupOf = new HashMap<>();
        int[] keyGroup = new int[n];
        int[] groupSlot = new int[n];
        int[] sizes = new int[n];
        int groups = 0;
        for (int i = 0; i < n; i++) {
            int slot = HashSlot.of(b, f.off[k[i]], f.len[k[i]]);
            Integer g = groupOf.get(slot);
            if (g == null) {
                g = groups++;
                groupOf.put(slot, g);
                groupSlot[g] = slot;
            }
            keyGroup[i] = g;
            sizes[g]++;
        }
        int[][] members = new int[groups][];
        for (int g = 0; g < groups; g++) {
            members[g] = new int[sizes[g]];
            sizes[g] = 0;
        }
        for (int i = 0; i < n; i++) {
            int g = keyGroup[i];
            members[g][sizes[g]++] = i;
        }
        SplitParent parent = new SplitParent(r, spec.split, n, members);
        RespOut tmp = new RespOut(256);
        for (int g = 0; g < groups; g++) {
            int[] m = members[g];
            tmp.reset();
            tmp.header(1 + m.length * (pairs ? 2 : 1));
            tmp.bulk(b, f.off[0], f.len[0]);
            for (int j : m) {
                int idx = k[j];
                tmp.bulk(b, f.off[idx], f.len[idx]);
                if (pairs) {
                    tmp.bulk(b, f.off[idx + 1], f.len[idx + 1]);
                }
            }
            SplitPart part = new SplitPart(parent, g, r);
            part.router = this;
            part.frame = Arrays.copyOf(tmp.buf, tmp.pos);
            try {
                sink.send(owner(groupSlot[g]), part.frame, 0, part.frame.length, part);
            } catch (RuntimeException e) {
                parent.fail(e);
                return;
            }
        }
    }

    /** Collects the replies of the sub-commands of a split command and completes the caller's request. */
    public static final class SplitParent {
        private final RoutedRequest<?> target;
        private final int mode;
        private final int keys;
        private final int[][] members;
        private final Object[] results;
        private final AtomicInteger remaining;
        private final AtomicBoolean failed = new AtomicBoolean();

        SplitParent(RoutedRequest<?> target, int mode, int keys, int[][] members) {
            this.target = target;
            this.mode = mode;
            this.keys = keys;
            this.members = members;
            this.results = new Object[members.length];
            this.remaining = new AtomicInteger(members.length);
        }

        void part(int group, Object value) {
            results[group] = value;
            if (remaining.decrementAndGet() == 0 && !failed.get()) {
                try {
                    target.deliver(assemble());
                } catch (RuntimeException e) {
                    fail(e);
                }
            }
        }

        private Object assemble() {
            switch (mode) {
                case ClusterKeys.SPLIT_SUM: {
                    long sum = 0;
                    for (Object v : results) {
                        if (v instanceof Long) {
                            sum += (Long) v;
                        }
                    }
                    return sum;
                }
                case ClusterKeys.SPLIT_LIST: {
                    Object[] all = new Object[keys];
                    for (int g = 0; g < members.length; g++) {
                        List<?> part = (List<?>) results[g];
                        int[] m = members[g];
                        if (part == null || part.size() != m.length) {
                            throw new RuvioException("cluster split: unexpected reply size from a node");
                        }
                        for (int j = 0; j < m.length; j++) {
                            all[m[j]] = part.get(j);
                        }
                    }
                    return new ArrayList<>(Arrays.asList(all));
                }
                default:
                    return null;
            }
        }

        void fail(Throwable error) {
            if (failed.compareAndSet(false, true)) {
                target.failFinal(error);
            }
        }
    }

    /** One sub-command of a split command; it can follow a MOVED redirect like any routed request. */
    public static final class SplitPart extends RoutedRequest<Object> {
        private final SplitParent parent;
        private final int group;

        @SuppressWarnings("unchecked")
        SplitPart(SplitParent parent, int group, Request<?> like) {
            super((Reply<Object>) like.reply, like.keys, like.values);
            this.parent = parent;
            this.group = group;
        }

        @Override
        public void deliver(Object value) {
            parent.part(group, value);
        }

        @Override
        public void failFinal(Throwable error) {
            parent.fail(error);
        }
    }

    // ---- MOVED -----------------------------------------------------------------------------

    public static boolean isMoved(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && m.startsWith("MOVED ");
    }

    /** Follows a MOVED reply: patch/refresh the table, then send the saved frame to the new owner. */
    public void redirect(RoutedRequest<?> r, Throwable moved) {
        try {
            BACKGROUND.execute(() -> followMoved(r, moved));
        } catch (RejectedExecutionException e) {
            r.failFinal(moved);
        }
    }

    private void followMoved(RoutedRequest<?> r, Throwable error) {
        try {
            String[] parts = error.getMessage().split(" ");
            if (parts.length < 3) {
                r.failFinal(error);
                return;
            }
            int slot = Integer.parseInt(parts[1]);
            String addr = parts[2];
            int colon = addr.lastIndexOf(':');
            String host = colon > 0 ? addr.substring(0, colon) : seed.host;
            int port = Integer.parseInt(addr.substring(colon + 1));
            ClusterNode target = onMoved(slot, host, port, r.redirects > 1);
            DIRECT.send(target, r.frame, 0, r.frame.length, r);
        } catch (RuntimeException e) {
            r.failFinal(e instanceof NumberFormatException ? error : e);
        }
    }

    /**
     * Points the slot at the named node and returns that node, which the retry always goes to. Re-reads the
     * whole table first when that node is unknown or {@code again} (the command's second MOVED or later).
     */
    private ClusterNode onMoved(int slot, String host, int port, boolean again) {
        ClusterNode n = nodes.get(ClusterNode.address(host, port));
        boolean unknown = n == null;
        if (unknown) {
            n = addNode(host, port);
        }
        if (slot >= 0 && slot < SLOTS) {
            table[slot] = n;
        }
        if (unknown || again) {
            try {
                refresh();
            } catch (RuntimeException ignored) {
                // keep the patched slot; the next MOVED tries again
            }
        }
        return n;
    }

    /** Test hook: overwrites the owner of a slot, for example to simulate a stale table. */
    public void forceSlot(int slot, ClusterNode node) {
        table[slot] = node;
    }

    @Override
    public void close() {
        closed = true;
        for (ClusterNode n : nodes.values()) {
            n.close();
        }
    }

    public boolean isClosed() {
        return closed;
    }
}
