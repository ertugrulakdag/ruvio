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
    /**
     * Serializes a value to JSON text.
     *
     * @param value the object to serialize
     * @return the JSON text
     */
    String toJson(Object value);

    /**
     * Parses JSON text into an object.
     *
     * @param json the JSON text
     * @param type the target class
     * @param <T> the target type
     * @return the parsed object
     */
    <T> T fromJson(String json, Class<T> type);

    /**
     * Passthrough codec: values must already be JSON text (any CharSequence) and can only be read
     * back as String. This is the default. Any other type throws {@link IllegalArgumentException}.
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
