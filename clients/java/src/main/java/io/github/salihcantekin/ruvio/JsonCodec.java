// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * Pluggable JSON (de)serializer for the typed JSON helpers. The JDK has no JSON library and this
 * client has no dependencies, so plug in your own, for example with Jackson:
 *
 * <pre>{@code
 * ObjectMapper mapper = new ObjectMapper();
 * JsonCodec jackson = new JsonCodec() {
 *     public String toJson(Object value) {
 *         try { return mapper.writeValueAsString(value); } catch (Exception e) { throw new RuntimeException(e); }
 *     }
 *     public <T> T fromJson(String json, Class<T> type) {
 *         try { return mapper.readValue(json, type); } catch (Exception e) { throw new RuntimeException(e); }
 *     }
 * };
 * client.setJsonCodec(jackson);
 * }</pre>
 */
public interface JsonCodec {
    String toJson(Object value);

    <T> T fromJson(String json, Class<T> type);

    /**
     * Passthrough codec: values must already be JSON text (any CharSequence) and can only be read
     * back as String. This is the default.
     */
    JsonCodec PASSTHROUGH = new JsonCodec() {
        @Override
        public String toJson(Object value) {
            if (value instanceof CharSequence) {
                return value.toString();
            }
            throw new IllegalArgumentException("PASSTHROUGH JsonCodec takes JSON text; plug in a JsonCodec for " + value.getClass().getName());
        }

        @Override
        public <T> T fromJson(String json, Class<T> type) {
            if (type == String.class || type == Object.class || type == CharSequence.class) {
                return type.cast(json);
            }
            throw new IllegalArgumentException("PASSTHROUGH JsonCodec reads only String; plug in a JsonCodec for " + type.getName());
        }
    };
}
