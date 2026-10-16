// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioAsync;
import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.Transaction;
import io.github.salihcantekin.ruvio.RuvioPubSub;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Fails when a command of docs/compatibility-matrix.json has no typed method. The "java" field of
 * clients/testdata/command_methods.json maps each matrix command to "method" (on RuvioClient,
 * RuvioAsync and Pipeline) or "Class#method".
 */
class CoverageTest {
    private static final Pattern ENTRY = Pattern.compile("\"([^\"]+)\"\\s*:\\s*\\{([^{}]*)\\}");
    private static final Pattern JAVA = Pattern.compile("\"java\"\\s*:\\s*(\"[^\"]*\"|\\[[^\\]]*\\])");
    private static final Pattern STRING = Pattern.compile("\"([^\"]*)\"");

    /**
     * Matrix command to the "java" field of clients/testdata/command_methods.json: one or more
     * methods, each "method" (on RuvioClient, RuvioAsync and Pipeline) or "Class#method".
     */
    static Map<String, List<String>> table() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../testdata/command_methods.json").normalize();
        Matcher entry = ENTRY.matcher(Files.readString(p, StandardCharsets.UTF_8));
        Map<String, List<String>> m = new LinkedHashMap<>();
        while (entry.find()) {
            List<String> methods = new ArrayList<>();
            Matcher java = JAVA.matcher(entry.group(2));
            if (java.find()) {
                Matcher name = STRING.matcher(java.group(1));
                while (name.find()) {
                    methods.add(name.group(1));
                }
            }
            m.put(entry.group(1), methods);
        }
        return m;
    }

    static List<String> matrixCommands() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../../docs/compatibility-matrix.json").normalize();
        String json = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        int start = json.indexOf("\"commands\"");
        int end = json.indexOf("\"unsupported\"");
        String section = json.substring(start, end);
        Matcher m = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"").matcher(section);
        List<String> names = new ArrayList<>();
        while (m.find()) {
            names.add(m.group(1));
        }
        return names;
    }

    private static boolean hasPublic(Class<?> type, String method) {
        for (Method m : type.getMethods()) {
            if (m.getName().equals(method) && Modifier.isPublic(m.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    @Test
    void everyMatrixCommandHasATypedMethod() throws IOException {
        List<String> commands = matrixCommands();
        Map<String, List<String>> table = table();
        List<String> missing = new ArrayList<>();
        int covered = 0;
        for (String command : commands) {
            List<String> targets = table.get(command);
            if (targets == null || targets.isEmpty()) {
                missing.add(command + " (no command_methods.json entry)");
                continue;
            }
            boolean all = true;
            for (String target : targets) {
                boolean ok;
                if (target.contains("#")) {
                    String[] parts = target.split("#");
                    Class<?> type;
                    switch (parts[0]) {
                        case "RuvioClient": type = RuvioClient.class; break;
                        case "Transaction": type = Transaction.class; break;
                        case "RuvioPubSub": type = RuvioPubSub.class; break;
                        default: throw new IllegalStateException(target);
                    }
                    ok = hasPublic(type, parts[1]);
                } else {
                    ok = hasPublic(RuvioClient.class, target) && hasPublic(RuvioAsync.class, target)
                            && hasPublic(Pipeline.class, target);
                }
                if (!ok) {
                    missing.add(command + " -> " + target);
                    all = false;
                }
            }
            if (all) {
                covered++;
            }
        }
        System.out.println("[coverage] typed methods for " + covered + "/" + commands.size() + " matrix commands");
        assertTrue(missing.isEmpty(), "commands without a typed method: " + missing);
        Set<String> extra = new LinkedHashSet<>(table.keySet());
        extra.removeAll(commands);
        assertTrue(extra.isEmpty(), "command_methods.json entries not in the matrix: " + extra);
    }

    /**
     * Every one of the matrix commands must have a routing rule in the server's command table (key
     * positions themselves are checked against the server in {@link KeyVectorsTest}), and no command
     * the matrix marks "same_slot" may be routed by its first argument alone.
     */
    @Test
    void everyMatrixCommandHasAClusterRoutingRule() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../../docs/compatibility-matrix.json").normalize();
        String json = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        String section = json.substring(json.indexOf("\"commands\""), json.indexOf("\"unsupported\""));
        Matcher entry = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"(.*?)\"same_slot\"\\s*:\\s*(true|false)", Pattern.DOTALL).matcher(section);
        int commands = 0;
        int sameSlot = 0;
        List<String> problems = new ArrayList<>();
        while (entry.find()) {
            commands++;
            String word = entry.group(1).split(" ")[0];
            if (!CommandKeyRules.RULES.containsKey(word)) {
                problems.add(word + " is not in the command key table");
            }
            if (Boolean.parseBoolean(entry.group(3))) {
                sameSlot++;
                if (ClusterKeys.spec(word).first) {
                    problems.add(word + " is same_slot in the matrix but is routed by its first argument only");
                }
            }
        }
        assertTrue(problems.isEmpty(), problems.toString());
        System.out.println("[cluster] routing rule for " + commands + " matrix commands (" + sameSlot + " flagged same_slot)");
    }
}
