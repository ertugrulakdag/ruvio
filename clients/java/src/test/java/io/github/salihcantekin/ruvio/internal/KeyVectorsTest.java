// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The cluster key lookup must return exactly the keys the server routes each command in
 * clients/testdata/key_vectors.json by. The sharded Pub/Sub commands are routed by their channel
 * on purpose, so they are left out (the file lists no keys for them).
 */
class KeyVectorsTest {
    private static final Set<String> CHANNEL_ROUTED = Set.of("SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE");
    private static final Pattern VECTOR = Pattern.compile(
            "\"args\"\\s*:\\s*\\[([^\\]]*)\\]\\s*,\\s*\"keys\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern STRING = Pattern.compile("\"([^\"]*)\"");

    private static List<String> strings(String list) {
        List<String> out = new ArrayList<>();
        Matcher m = STRING.matcher(list);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    private static List<String> keysOf(List<String> args) {
        Frame f = new Frame();
        f.out.header(args.size());
        for (String a : args) {
            f.out.bulk(a);
        }
        f.parse();
        int n = ClusterKeys.keys(ClusterKeys.spec(f.out.buf, f.off[0], f.len[0]), f);
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(args.get(f.keyIdx[i]));
        }
        return out;
    }

    @Test
    void clusterKeysMatchTheServerVectors() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../testdata/key_vectors.json").normalize();
        Matcher m = VECTOR.matcher(Files.readString(p, StandardCharsets.UTF_8));
        int count = 0;
        List<String> problems = new ArrayList<>();
        while (m.find()) {
            List<String> args = strings(m.group(1));
            if (CHANNEL_ROUTED.contains(args.get(0).toUpperCase(Locale.ROOT))) {
                continue;
            }
            List<String> expected = strings(m.group(2));
            List<String> actual = keysOf(args);
            Collections.sort(actual);
            if (!actual.equals(expected)) {
                problems.add(args + ": got " + actual + ", expected " + expected);
            }
            count++;
        }
        System.out.println("[cluster] key vectors: " + (count - problems.size()) + "/" + count + " match");
        assertTrue(count > 1000, "found " + count + " vectors");
        assertTrue(problems.isEmpty(), problems.size() + " of " + count + " vectors differ:\n" + String.join("\n", problems));
    }
}
