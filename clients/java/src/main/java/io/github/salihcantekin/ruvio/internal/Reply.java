// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared, stateless reply decoders. Scalar decoders read straight from the connection buffer;
 * aggregate shapes that need models decode a generic tree first and convert it.
 */
public abstract class Reply<T> {
    public abstract T decode(RespReader r, Request<?> q);

    /** Converts a generic reply tree; see {@link RespReader#readObject()} for the tree types. */
    public interface Converter<T> {
        T convert(Object tree, Request<?> q);
    }

    public static <T> Reply<T> of(Converter<T> converter) {
        return new Reply<T>() {
            @Override
            public T decode(RespReader r, Request<?> q) {
                Object tree = r.readObject();
                if (tree instanceof RuvioServerException) {
                    throw (RuvioServerException) tree;
                }
                return converter.convert(tree, q);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> Reply<T> cast(Reply<?> reply) {
        return (Reply<T>) reply;
    }

    // ---- scalars --------------------------------------------------------------------------

    /** +OK (or any status); nil gives null. */
    public static final Reply<Void> OK = new Reply<Void>() {
        @Override
        public Void decode(RespReader r, Request<?> q) {
            r.stringSpan();
            return null;
        }
    };

    /** +OK -> true, nil -> false (conditional writes). */
    public static final Reply<Boolean> OK_BOOL = new Reply<Boolean>() {
        @Override
        public Boolean decode(RespReader r, Request<?> q) {
            return r.stringSpan() >= 0 ? Boolean.TRUE : Boolean.FALSE;
        }
    };

    public static final Reply<String> STRING = new Reply<String>() {
        @Override
        public String decode(RespReader r, Request<?> q) {
            return r.readString();
        }
    };

    public static final Reply<byte[]> BYTES = new Reply<byte[]>() {
        @Override
        public byte[] decode(RespReader r, Request<?> q) {
            return r.readBytes();
        }
    };

    public static final Reply<Long> LONG = new Reply<Long>() {
        @Override
        public Long decode(RespReader r, Request<?> q) {
            return r.readLongOrNull();
        }
    };

    /** Integer reply, 0 -> false, anything else -> true; nil -> false. */
    public static final Reply<Boolean> BOOL = new Reply<Boolean>() {
        @Override
        public Boolean decode(RespReader r, Request<?> q) {
            Long v = r.readLongOrNull();
            return v != null && v != 0;
        }
    };

    public static final Reply<Double> DOUBLE = new Reply<Double>() {
        @Override
        public Double decode(RespReader r, Request<?> q) {
            return r.readDoubleOrNull();
        }
    };

    public static final Reply<Object> VALUE = new Reply<Object>() {
        @Override
        public Object decode(RespReader r, Request<?> q) {
            return r.readBulk(q.values);
        }
    };

    public static final Reply<Object> KEY = new Reply<Object>() {
        @Override
        public Object decode(RespReader r, Request<?> q) {
            return r.readBulk(q.keys);
        }
    };

    public static final Reply<Object> RAW = new Reply<Object>() {
        @Override
        public Object decode(RespReader r, Request<?> q) {
            return r.readObject();
        }
    };

    // ---- arrays decoded in place ----------------------------------------------------------

    public static final Reply<List<Object>> VALUE_LIST = new Reply<List<Object>>() {
        @Override
        public List<Object> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(r.readBulk(q.values));
            }
            return out;
        }
    };

    /** Like VALUE_LIST but a nil array gives null (blocking pops, LMPOP, ...). */
    public static final Reply<List<Object>> KEY_LIST = new Reply<List<Object>>() {
        @Override
        public List<Object> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(r.readBulk(q.keys));
            }
            return out;
        }
    };

    public static final Reply<List<String>> STRING_LIST = new Reply<List<String>>() {
        @Override
        public List<String> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<String> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(r.readString());
            }
            return out;
        }
    };

    public static final Reply<List<Long>> LONG_LIST = new Reply<List<Long>>() {
        @Override
        public List<Long> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<Long> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(r.readLongOrNull());
            }
            return out;
        }
    };

    public static final Reply<List<Boolean>> BOOL_LIST = new Reply<List<Boolean>>() {
        @Override
        public List<Boolean> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<Boolean> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                Long v = r.readLongOrNull();
                out.add(v != null && v != 0);
            }
            return out;
        }
    };

    public static final Reply<List<Double>> DOUBLE_LIST = new Reply<List<Double>>() {
        @Override
        public List<Double> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyList();
            }
            List<Double> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(r.readDoubleOrNull());
            }
            return out;
        }
    };

    /** Flat key/value pairs (or a RESP3 map) into an insertion-ordered map of K to V. */
    public static final Reply<Map<Object, Object>> KV_MAP = new Reply<Map<Object, Object>>() {
        @Override
        public Map<Object, Object> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyMap();
            }
            Map<Object, Object> out = new LinkedHashMap<>(Math.max(4, n));
            for (int i = 0; i < n; i += 2) {
                Object k = r.readBulk(q.keys);
                out.put(k, r.readBulk(q.values));
            }
            return out;
        }
    };

    public static final Reply<Map<String, String>> STRING_MAP = new Reply<Map<String, String>>() {
        @Override
        public Map<String, String> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n < 0) {
                return Collections.emptyMap();
            }
            Map<String, String> out = new LinkedHashMap<>(Math.max(4, n));
            for (int i = 0; i < n; i += 2) {
                String k = r.readString();
                out.put(k, r.readString());
            }
            return out;
        }
    };

    /** member, score, member, score ... -> list of ScoredValue. */
    public static final Reply<List<ScoredValue<Object>>> SCORED = new Reply<List<ScoredValue<Object>>>() {
        @Override
        public List<ScoredValue<Object>> decode(RespReader r, Request<?> q) {
            int n = r.arrayLength();
            if (n <= 0) {
                return Collections.emptyList();
            }
            byte t = r.peek();
            List<ScoredValue<Object>> out = new ArrayList<>(n / 2);
            if (t == '*') {
                // RESP3 shape: array of [member, score] pairs
                for (int i = 0; i < n; i++) {
                    r.arrayLength();
                    Object v = r.readBulk(q.values);
                    out.add(new ScoredValue<>(v, r.readDoubleOrNull()));
                }
                return out;
            }
            for (int i = 0; i < n; i += 2) {
                Object v = r.readBulk(q.values);
                Double s = r.readDoubleOrNull();
                out.add(new ScoredValue<>(v, s == null ? Double.NaN : s));
            }
            return out;
        }
    };

    // ---- helpers for converters -----------------------------------------------------------

    public static String str(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[]) {
            return new String((byte[]) o, StandardCharsets.UTF_8);
        }
        return o.toString();
    }

    public static long lng(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        }
        if (o instanceof byte[]) {
            byte[] b = (byte[]) o;
            return (long) RespReader.parseDouble(b, 0, b.length);
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        if (o == null) {
            return 0;
        }
        return Long.parseLong(o.toString());
    }

    public static Double dbl(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[]) {
            byte[] b = (byte[]) o;
            return RespReader.parseDouble(b, 0, b.length);
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        byte[] b = o.toString().getBytes(StandardCharsets.UTF_8);
        return RespReader.parseDouble(b, 0, b.length);
    }

    @SuppressWarnings("unchecked")
    public static List<Object> list(Object o) {
        if (o == null) {
            return Collections.emptyList();
        }
        if (o instanceof List) {
            return (List<Object>) o;
        }
        if (o instanceof Map) {
            List<Object> flat = new ArrayList<>();
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) o).entrySet()) {
                flat.add(e.getKey());
                flat.add(e.getValue());
            }
            return flat;
        }
        throw new RuvioException("unexpected reply " + o);
    }

    @SuppressWarnings("unchecked")
    public static <T> T decodeWith(RuvioCodec<?> codec, Object o) {
        if (o == null) {
            return null;
        }
        byte[] b = o instanceof byte[] ? (byte[]) o : o.toString().getBytes(StandardCharsets.UTF_8);
        return (T) codec.decode(b, 0, b.length);
    }

    /** Flat pairs (or RESP3 map) into a String-keyed map; values keep their tree type. */
    public static Map<String, Object> fieldMap(Object o) {
        List<Object> items = list(o);
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i + 1 < items.size(); i += 2) {
            out.put(str(items.get(i)), items.get(i + 1));
        }
        return out;
    }

    /** Recursively turns byte[] leaves into UTF-8 Strings, for human-oriented replies. */
    @SuppressWarnings("unchecked")
    public static Object text(Object o) {
        if (o instanceof byte[]) {
            return str(o);
        }
        if (o instanceof List) {
            List<Object> in = (List<Object>) o;
            List<Object> out = new ArrayList<>(in.size());
            for (Object x : in) {
                out.add(text(x));
            }
            return out;
        }
        if (o instanceof Map) {
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) o).entrySet()) {
                out.put(text(e.getKey()), text(e.getValue()));
            }
            return out;
        }
        return o;
    }

    public static final Reply<Object> TEXT = of((o, q) -> text(o));
}
