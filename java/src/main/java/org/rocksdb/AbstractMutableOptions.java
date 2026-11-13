// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import java.util.*;

/**
 * This class is not strictly abstract in Java language terms, so we do not declare it as such.
 * The name remains {@code AbstractMutableOptions} to reflect the underlying C++ name.
 * The constructor is protected, so it will always be used as a base class.
 */
public class AbstractMutableOptions {
  protected static final String KEY_VALUE_PAIR_SEPARATOR = ";";
  protected static final char KEY_VALUE_SEPARATOR = '=';
  static final String INT_ARRAY_INT_SEPARATOR = ":";

  private static final String HAS_NOT_BEEN_SET = " has not been set";

  protected final String[] keys;
  private final String[] values;

  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   */
  @SuppressWarnings("PMD.ArrayIsStoredDirectly")
  protected AbstractMutableOptions(final String[] keys, final String[] values) {
    this.keys = keys;
    this.values = values;
  }

  @SuppressWarnings("PMD.MethodReturnsInternalArray")
  String[] getKeys() {
    return keys;
  }

  @SuppressWarnings("PMD.MethodReturnsInternalArray")
  String[] getValues() {
    return values;
  }

  /**
   * Returns a string representation of MutableOptions which
   * is suitable for consumption by {@code #parse(String)}.
   *
   * @return String representation of MutableOptions
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    for(int i = 0; i < keys.length; i++) {
      buffer
          .append(keys[i])
          .append(KEY_VALUE_SEPARATOR)
          .append(values[i]);

      if(i + 1 < keys.length) {
        buffer.append(KEY_VALUE_PAIR_SEPARATOR);
      }
    }
    return buffer.toString();
  }

  public abstract static class AbstractMutableOptionsBuilder<
      T extends AbstractMutableOptions, U extends AbstractMutableOptionsBuilder<T, U, K>, K
          extends MutableOptionKey> {
    private final Map<K, MutableOptionValue<?>> options = new LinkedHashMap<>();
    private final List<OptionString.Entry> unknown = new ArrayList<>();

    protected abstract U self();

    /**
     * Get all the possible keys
     *
     * @return A map of all keys, indexed by name.
     */
    protected abstract Map<String, K> allKeys();

    /**
     * Construct a subclass instance of {@link AbstractMutableOptions}.
     *
     * @param keys the keys
     * @param values the values
     *
     * @return an instance of the options.
     */
    protected abstract T build(final String[] keys, final String[] values);

    public T build() {
      final String[] keys = new String[options.size()];
      final String[] values = new String[options.size()];

      int i = 0;
      for (final Map.Entry<K, MutableOptionValue<?>> option : options.entrySet()) {
        keys[i] = option.getKey().name();
        values[i] = option.getValue().asString();
        i++;
      }

      return build(keys, values);
    }

    protected U setDouble(
       final K key, final double value) {
      if (key.getValueType() != MutableOptionKey.ValueType.DOUBLE) {
        throw new IllegalArgumentException(
            key + " does not accept a double value");
      }
      options.put(key, MutableOptionValue.fromDouble(value));
      return self();
    }

    protected double getDouble(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asDouble();
    }

    protected U setLong(
        final K key, final long value) {
      if(key.getValueType() != MutableOptionKey.ValueType.LONG) {
        throw new IllegalArgumentException(
            key + " does not accept a long value");
      }
      options.put(key, MutableOptionValue.fromLong(value));
      return self();
    }

    protected long getLong(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asLong();
    }

    protected U setInt(
        final K key, final int value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT) {
        throw new IllegalArgumentException(
            key + " does not accept an integer value");
      }
      options.put(key, MutableOptionValue.fromInt(value));
      return self();
    }

    protected int getInt(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asInt();
    }

    protected U setBoolean(
        final K key, final boolean value) {
      if(key.getValueType() != MutableOptionKey.ValueType.BOOLEAN) {
        throw new IllegalArgumentException(
            key + " does not accept a boolean value");
      }
      options.put(key, MutableOptionValue.fromBoolean(value));
      return self();
    }

    protected boolean getBoolean(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asBoolean();
    }

    protected U setIntArray(
        final K key, final int[] value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT_ARRAY) {
        throw new IllegalArgumentException(
            key + " does not accept an int array value");
      }
      options.put(key, MutableOptionValue.fromIntArray(value));
      return self();
    }

    protected int[] getIntArray(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asIntArray();
    }

    protected U setString(final K key, final String value) {
      if (key.getValueType() != MutableOptionKey.ValueType.STRING) {
        throw new IllegalArgumentException(key + " does not accept a string value");
      }
      options.put(key, MutableOptionValue.fromString(value));
      return self();
    }

    protected String getString(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if (value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }
      return value.asString();
    }

    protected <N extends Enum<N>> U setEnum(
        final K key, final N value) {
      if(key.getValueType() != MutableOptionKey.ValueType.ENUM) {
        throw new IllegalArgumentException(
            key + " does not accept a Enum value");
      }
      options.put(key, MutableOptionValue.fromEnum(value));
      return self();
    }

    @SuppressWarnings("unchecked")
    protected <N extends Enum<N>> N getEnum(final K key)
        throws NoSuchElementException, NumberFormatException {
      final MutableOptionValue<?> value = options.get(key);
      if (value == null) {
        throw new NoSuchElementException(key.name() + HAS_NOT_BEEN_SET);
      }

      if (!(value instanceof MutableOptionValue.MutableOptionEnumValue)) {
        throw new NoSuchElementException(key.name() + " is not of Enum type");
      }

      return ((MutableOptionValue.MutableOptionEnumValue<N>) value).asObject();
    }

    /**
     * Parse a string into a long value, accepting values expressed as a double (such as 9.00) which
     * are meant to be a long, not a double
     *
     * @param value the string containing a value which represents a long
     * @return the long value of the parsed string
     */
    private long parseAsLong(final String value) {
      try {
        return Long.parseLong(value);
      } catch (final NumberFormatException nfe) {
        final double doubleValue = Double.parseDouble(value);
        if (doubleValue != Math.round(doubleValue))
          throw new IllegalArgumentException("Unable to parse or round " + value + " to long", nfe);
        return Math.round(doubleValue);
      }
    }

    /**
     * Parse a string into an int value, accepting values expressed as a double (such as 9.00) which
     * are meant to be an int, not a double
     *
     * @param value the string containing a value which represents an int
     * @return the int value of the parsed string
     */
    private int parseAsInt(final String value) {
      try {
        return Integer.parseInt(value);
      } catch (final NumberFormatException nfe) {
        final double doubleValue = Double.parseDouble(value);
        if (doubleValue != Math.round(doubleValue))
          throw new IllegalArgumentException("Unable to parse or round " + value + " to int", nfe);
        return (int) Math.round(doubleValue);
      }
    }

    /**
     * Constructs a builder for mutable column family options from a hierarchical parsed options
     * string representation. The {@link OptionString.Parser} class output has been used to create a
     * (name,value)-list; each value may be either a simple string or a (name, value)-list in turn.
     *
     * @param options a list of parsed option string objects
     * @param ignoreUnknown what to do if the key is not one of the keys we expect
     *
     * @return a builder with the values from the parsed input set
     *
     * @throws IllegalArgumentException if an option value is of the wrong type, or a key is empty
     */
    protected U fromParsed(final List<OptionString.Entry> options, final boolean ignoreUnknown) {
      Objects.requireNonNull(options);

      for (final OptionString.Entry option : options) {
        try {
          if (option.key.isEmpty()) {
            throw new IllegalArgumentException("options string is invalid: " + option);
          }
          fromOptionString(option, ignoreUnknown);
        } catch (final NumberFormatException nfe) {
          throw new IllegalArgumentException(
              "" + option.key + "=" + option.value + " - not a valid value for its type", nfe);
        }
      }

      return self();
    }

    /**
     * Set a value in the builder from the supplied option string
     *
     * @param option the option key/value to add to this builder
     * @param ignoreUnknown if this is not set, throw an exception when a key is not in the known
     *     set
     * @return the same object, after adding options
     * @throws IllegalArgumentException if the key is unknown, or a value has the wrong type/form
     */
    @SuppressWarnings({"PMD.ExhaustiveSwitchHasDefault", "PMD.AvoidLiteralsInIfCondition"})
    private U fromOptionString(final OptionString.Entry option, final boolean ignoreUnknown)
        throws IllegalArgumentException {
      Objects.requireNonNull(option.key);
      Objects.requireNonNull(option.value);

      final K key = allKeys().get(option.key);
      if (key == null && ignoreUnknown) {
        unknown.add(option);
        return self();
      } else if (key == null) {
        throw new IllegalArgumentException("Key: " + null + " is not a known option key");
      }

      if (!option.value.isList()) {
        throw new IllegalArgumentException(
            "Option: " + key + " is not a simple value or list, don't know how to parse it");
      }

      // Check that simple values are the single item in the array
      if (key.getValueType() != MutableOptionKey.ValueType.INT_ARRAY
          && key.getValueType() != MutableOptionKey.ValueType.STRING) {
        {
          if (option.value.list.size() != 1) {
            throw new IllegalArgumentException(
                "Simple value does not have exactly 1 item: " + option.value.list);
          }
        }
      }

      final List<String> valueStrs = option.value.list;
      final String valueStr = valueStrs.get(0);

      switch (key.getValueType()) {
        case DOUBLE:
          return setDouble(key, Double.parseDouble(valueStr));

        case LONG:
          return setLong(key, parseAsLong(valueStr));

        case INT:
          return setInt(key, parseAsInt(valueStr));

        case BOOLEAN:
          return setBoolean(key, Boolean.parseBoolean(valueStr));

        case INT_ARRAY:
          final int[] value = new int[valueStrs.size()];
          for (int i = 0; i < valueStrs.size(); i++) {
            value[i] = Integer.parseInt(valueStrs.get(i));
          }
          return setIntArray(key, value);

        case ENUM:
          final String optionName = key.name();
          if ("prepopulate_blob_cache".equals(optionName)) {
            final PrepopulateBlobCache prepopulateBlobCache =
                PrepopulateBlobCache.getFromInternal(valueStr);
            return setEnum(key, prepopulateBlobCache);
          } else if ("compression".equals(optionName)
              || "blob_compression_type".equals(optionName)) {
            final CompressionType compressionType = CompressionType.getFromInternal(valueStr);
            return setEnum(key, compressionType);
          } else {
            throw new IllegalArgumentException("Unknown enum type: " + key.name());
          }
        case STRING:
          return setString(key, option.value.toString());
        default:
          throw new IllegalStateException(key + " has unknown value type: " + key.getValueType());
      }
    }

    /**
     *
     * @return the list of keys encountered which were not known to the type being generated
     */
    public List<OptionString.Entry> getUnknown() {
      return new ArrayList<>(unknown);
    }
  }
}
