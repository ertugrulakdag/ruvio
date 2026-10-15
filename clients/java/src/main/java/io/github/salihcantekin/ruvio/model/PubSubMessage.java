// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.RuvioPubSub;

import java.nio.charset.StandardCharsets;

/** A message delivered to a {@link RuvioPubSub} listener. */
public final class PubSubMessage {
    /** message (SUBSCRIBE), pmessage (PSUBSCRIBE) or smessage (SSUBSCRIBE). */
    public enum Kind { MESSAGE, PMESSAGE, SMESSAGE }

    private final Kind kind;
    private final String pattern;
    private final String channel;
    private final byte[] payload;

    public PubSubMessage(Kind kind, String pattern, String channel, byte[] payload) {
        this.kind = kind;
        this.pattern = pattern;
        this.channel = channel;
        this.payload = payload;
    }

    public Kind kind() {
        return kind;
    }

    /** The matching pattern for {@link Kind#PMESSAGE}, otherwise null. */
    public String pattern() {
        return pattern;
    }

    public String channel() {
        return channel;
    }

    public byte[] payload() {
        return payload;
    }

    public String payloadString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return kind + " " + (pattern == null ? "" : pattern + " ") + channel + ": " + payloadString();
    }
}
