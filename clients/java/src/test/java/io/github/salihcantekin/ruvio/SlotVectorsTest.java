// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** The key to hash slot vectors in clients/testdata/slots.json are shared by the server and every client. */
class SlotVectorsTest {
    @Test
    void hashSlotMatchesTheSharedVectors() throws IOException {
        Path p = Paths.get(System.getProperty("user.dir")).resolve("../testdata/slots.json").normalize();
        String text = Files.readString(p, StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("\"key\"\\s*:\\s*\"([^\"]*)\"\\s*,\\s*\"slot\"\\s*:\\s*(\\d+)").matcher(text);
        int count = 0;
        while (m.find()) {
            String key = m.group(1);
            int slot = Integer.parseInt(m.group(2));
            assertEquals(slot, RuvioClient.hashSlot(key), key);
            assertEquals(slot, RuvioClient.hashSlot(key.getBytes(StandardCharsets.UTF_8)), key);
            count++;
        }
        assertTrue(count > 10, "found " + count + " vectors");
    }
}
