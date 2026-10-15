// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.RuvioAsync;
import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioPubSub;
import io.github.salihcantekin.ruvio.Transaction;
import io.github.salihcantekin.ruvio.args.ClientKillArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Every typed Ruvio command as an asynchronous method. Implemented by {@link RuvioAsync} (the shared
 * multiplexed connection), {@link Pipeline} and {@link Transaction}. This class adds connection,
 * ACL, server, cluster and publish commands to the data-type families it inherits.
 */
public abstract class RuvioCommands<K, V> extends ScriptingCommands<K, V> {
    protected RuvioCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    @SuppressWarnings("unchecked")
    static final Reply<Map<String, Object>> TEXT_MAP = Reply.of((tree, q) -> Reply.fieldMap(Reply.text(tree)));

    @SuppressWarnings("unchecked")
    static final Reply<List<Object>> TEXT_LIST = Reply.of((tree, q) -> (List<Object>) Reply.text(Reply.list(tree)));

    // ---- connection ------------------------------------------------------------------------

    /** RUVIO: "PONG is taken". */
    public CompletableFuture<String> ruvio() {
        return c0(Cmd.RUVIO, Reply.STRING);
    }

    /** RUVIO OWNER (or another single word): server-defined text. */
    public CompletableFuture<String> ruvio(String argument) {
        return send(args().w(Cmd.RUVIO).s(argument), Reply.STRING);
    }

    public CompletableFuture<String> ping() {
        return c0(Cmd.PING, Reply.STRING);
    }

    public CompletableFuture<V> ping(V message) {
        return send(args().w(Cmd.PING).v(message), r(Reply.VALUE));
    }

    public CompletableFuture<V> echo(V message) {
        return send(args().w(Cmd.ECHO).v(message), r(Reply.VALUE));
    }

    /**
     * HELLO protover: switches this connection's protocol (2 or 3) and returns the server map.
     * The reader understands both protocols; RESP3 pushes go to the push listener.
     */
    public CompletableFuture<Map<String, Object>> hello(int protocol) {
        return send(args().w(Cmd.HELLO).l(protocol), TEXT_MAP);
    }

    /** HELLO protover AUTH username password. */
    public CompletableFuture<Map<String, Object>> hello(int protocol, String username, String password) {
        return send(args().w(Cmd.HELLO).l(protocol).s("AUTH").s(username).s(password), TEXT_MAP);
    }

    public CompletableFuture<Void> auth(String password) {
        return send(args().w(Cmd.AUTH).s(password), Reply.OK);
    }

    public CompletableFuture<Void> auth(String username, String password) {
        return send(args().w(Cmd.AUTH).s(username).s(password), Reply.OK);
    }

    /** SELECT db. On the shared client this changes the database for every caller. */
    public CompletableFuture<Void> select(int database) {
        return send(args().w(Cmd.SELECT).l(database), Reply.OK);
    }

    /** COMMAND: the command table (Redis 7 shape), UTF-8 text. */
    public CompletableFuture<List<Object>> command() {
        return c0(Cmd.COMMAND, TEXT_LIST);
    }

    public CompletableFuture<Long> commandCount() {
        return send(args().w(Cmd.COMMAND).s("COUNT"), Reply.LONG);
    }

    public CompletableFuture<List<Object>> commandInfo(String... names) {
        return send(args().w(Cmd.COMMAND).s("INFO").strings(names), TEXT_LIST);
    }

    public CompletableFuture<Object> commandDocs(String... names) {
        return send(args().w(Cmd.COMMAND).s("DOCS").strings(names), Reply.TEXT);
    }

    /** CLIENT GETNAME: null when unnamed. */
    public CompletableFuture<String> clientGetname() {
        return send(args().w(Cmd.CLIENT).s("GETNAME"), Reply.STRING);
    }

    public CompletableFuture<Void> clientSetname(String name) {
        return send(args().w(Cmd.CLIENT).s("SETNAME").s(name), Reply.OK);
    }

    /** CLIENT TRACKING ON|OFF (ON requires RESP3: call {@code hello(3)} first). */
    public CompletableFuture<Void> clientTracking(boolean on) {
        return send(args().w(Cmd.CLIENT).s("TRACKING").s(on ? "ON" : "OFF"), Reply.OK);
    }

    public CompletableFuture<Long> clientId() {
        return send(args().w(Cmd.CLIENT).s("ID"), Reply.LONG);
    }

    public CompletableFuture<String> clientList() {
        return send(args().w(Cmd.CLIENT).s("LIST"), Reply.STRING);
    }

    public CompletableFuture<String> clientInfo() {
        return send(args().w(Cmd.CLIENT).s("INFO"), Reply.STRING);
    }

    /** CLIENT KILL with ID / ADDR / SKIPME filters: the number of killed connections. */
    public CompletableFuture<Long> clientKill(ClientKillArgs filter) {
        Args a = args().w(Cmd.CLIENT).s("KILL");
        filter.write(a);
        return send(a, Reply.LONG);
    }

    /** Legacy CLIENT KILL addr:port. */
    public CompletableFuture<Void> clientKill(String addr) {
        return send(args().w(Cmd.CLIENT).s("KILL").s(addr), Reply.OK);
    }

    /** SLOWLOG GET: entries as text lists. */
    public CompletableFuture<List<Object>> slowlogGet() {
        return send(args().w(Cmd.SLOWLOG).s("GET"), TEXT_LIST);
    }

    public CompletableFuture<List<Object>> slowlogGet(long count) {
        return send(args().w(Cmd.SLOWLOG).s("GET").l(count), TEXT_LIST);
    }

    public CompletableFuture<Long> slowlogLen() {
        return send(args().w(Cmd.SLOWLOG).s("LEN"), Reply.LONG);
    }

    public CompletableFuture<Void> slowlogReset() {
        return send(args().w(Cmd.SLOWLOG).s("RESET"), Reply.OK);
    }

    /** LATENCY LATEST: always empty on Ruvio. */
    public CompletableFuture<List<Object>> latencyLatest() {
        return send(args().w(Cmd.LATENCY).s("LATEST"), TEXT_LIST);
    }

    // ---- ACL -------------------------------------------------------------------------------

    public CompletableFuture<String> aclWhoami() {
        return send(args().w(Cmd.ACL).s("WHOAMI"), Reply.STRING);
    }

    public CompletableFuture<List<String>> aclUsers() {
        return send(args().w(Cmd.ACL).s("USERS"), Reply.STRING_LIST);
    }

    public CompletableFuture<List<String>> aclList() {
        return send(args().w(Cmd.ACL).s("LIST"), Reply.STRING_LIST);
    }

    /** ACL GETUSER: flat fields as a map (null user gives an empty map). */
    public CompletableFuture<Map<String, Object>> aclGetuser(String user) {
        return send(args().w(Cmd.ACL).s("GETUSER").s(user), TEXT_MAP);
    }

    public CompletableFuture<List<String>> aclCat() {
        return send(args().w(Cmd.ACL).s("CAT"), Reply.STRING_LIST);
    }

    public CompletableFuture<List<String>> aclCat(String category) {
        return send(args().w(Cmd.ACL).s("CAT").s(category), Reply.STRING_LIST);
    }

    /** ACL SETUSER user rule... (for example "on", "&gt;password", "~app:*", "+get"). */
    public CompletableFuture<Void> aclSetuser(String user, String... rules) {
        return send(args().w(Cmd.ACL).s("SETUSER").s(user).strings(rules), Reply.OK);
    }

    public CompletableFuture<Void> aclLoad() {
        return send(args().w(Cmd.ACL).s("LOAD"), Reply.OK);
    }

    public CompletableFuture<Void> aclSave() {
        return send(args().w(Cmd.ACL).s("SAVE"), Reply.OK);
    }

    // ---- server ----------------------------------------------------------------------------

    public CompletableFuture<String> info() {
        return c0(Cmd.INFO, Reply.STRING);
    }

    /** INFO section ("prometheus" returns Prometheus text). */
    public CompletableFuture<String> info(String section) {
        return send(args().w(Cmd.INFO).s(section), Reply.STRING);
    }

    /** CONFIG GET pattern: name -> value (requirepass is redacted). */
    public CompletableFuture<Map<String, String>> configGet(String pattern) {
        return send(args().w(Cmd.CONFIG).s("GET").s(pattern), Reply.STRING_MAP);
    }

    public CompletableFuture<Void> save() {
        return c0(Cmd.SAVE, Reply.OK);
    }

    /** BGSAVE: the status text ("Background saving started"). */
    public CompletableFuture<String> bgsave() {
        return c0(Cmd.BGSAVE, Reply.STRING);
    }

    /** WAIT numreplicas timeout_ms: replicas that reached this shard's WAL sequence. */
    public CompletableFuture<Long> waitReplicas(long replicas, long timeoutMillis) {
        return send(args().w(Cmd.WAIT).l(replicas).l(timeoutMillis), Reply.LONG);
    }

    // ---- cluster ---------------------------------------------------------------------------

    public CompletableFuture<List<Object>> clusterSlots() {
        return send(args().w(Cmd.CLUSTER).s("SLOTS"), TEXT_LIST);
    }

    public CompletableFuture<String> clusterNodes() {
        return send(args().w(Cmd.CLUSTER).s("NODES"), Reply.STRING);
    }

    public CompletableFuture<List<Object>> clusterShards() {
        return send(args().w(Cmd.CLUSTER).s("SHARDS"), TEXT_LIST);
    }

    public CompletableFuture<String> clusterInfo() {
        return send(args().w(Cmd.CLUSTER).s("INFO"), Reply.STRING);
    }

    public CompletableFuture<String> clusterMyid() {
        return send(args().w(Cmd.CLUSTER).s("MYID"), Reply.STRING);
    }

    public CompletableFuture<Long> clusterKeyslot(K key) {
        return send(args().w(Cmd.CLUSTER).s("KEYSLOT").k(key), Reply.LONG);
    }

    public CompletableFuture<Void> readonly() {
        return c0(Cmd.READONLY, Reply.OK);
    }

    public CompletableFuture<Void> readwrite() {
        return c0(Cmd.READWRITE, Reply.OK);
    }

    // ---- publish (subscribing needs a RuvioPubSub connection) ------------------------------

    /** PUBLISH channel message: receivers on this shard. */
    public CompletableFuture<Long> publish(String channel, V message) {
        return send(args().w(Cmd.PUBLISH).s(channel).v(message), Reply.LONG);
    }

    /** SPUBLISH shardchannel message. */
    public CompletableFuture<Long> spublish(String channel, V message) {
        return send(args().w(Cmd.SPUBLISH).s(channel).v(message), Reply.LONG);
    }
}
