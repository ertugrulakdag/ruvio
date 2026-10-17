// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

import io.github.salihcantekin.ruvio.RuvioPubSub;

import java.nio.charset.StandardCharsets;

/**
 * A message delivered to a {@link RuvioPubSub} listener: which kind of subscription matched, the
 * channel and the payload.
 */
public final class PubSubMessage {
    /** How a message was received: from SUBSCRIBE ({@code MESSAGE}), PSUBSCRIBE ({@code PMESSAGE}) or SSUBSCRIBE ({@code SMESSAGE}). */
    public enum Kind {
        /** Received through {@code SUBSCRIBE} on an exact channel. */
        MESSAGE,
        /** Received through {@code PSUBSCRIBE}; {@link PubSubMessage#pattern()} holds the matching pattern. */
        PMESSAGE,
        /** Received through {@code SSUBSCRIBE} on a shard channel. */
        SMESSAGE
    }

    private final Kind kind;
    private final String pattern;
    private final String channel;
    private final byte[] payload;

    /**
     * Creates a message.
     *
     * @param kind how the message was received
     * @param pattern the matching pattern for {@code PMESSAGE}, otherwise {@code null}
     * @param channel the channel it was published to
     * @param payload the message bytes
     */
    public PubSubMessage(Kind kind, String pattern, String channel, byte[] payload) {
        this.kind = kind;
        this.pattern = pattern;
        this.channel = channel;
        this.payload = payload;
    }

    /**
     * Returns how the message was received.
     *
     * @return the kind
     */
    public Kind kind() {
        return kind;
    }

    /**
     * Returns the matching pattern.
     *
     * @return the pattern for {@code PMESSAGE}, otherwise {@code null}
     */
    public String pattern() {
        return pattern;
    }

    /**
     * Returns the channel the message was published to.
     *
     * @return the channel name
     */
    public String channel() {
        return channel;
    }

    /**
     * Returns the message bytes.
     *
     * @return the payload as sent
     */
    public byte[] payload() {
        return payload;
    }

    /**
     * Returns the message decoded as UTF-8 text.
     *
     * @return the payload as a string
     */
    public String payloadString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /**
     * Returns a short readable form, for logs.
     *
     * @return the text
     */
    @Override
    public String toString() {
        return kind + " " + (pattern == null ? "" : pattern + " ") + channel + ": " + payloadString();
    }
}
