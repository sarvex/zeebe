/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import static io.camunda.zeebe.db.impl.ZeebeDbConstants.ZB_DB_BYTE_ORDER;

import io.camunda.zeebe.db.DbKey;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.ObjIntConsumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class NewColumnFamilyContext<KeyType, ValueType> {
  static Map<Class, TypeEncoderDecoder> encoderDecoderMap = new HashMap<>();
  private static final byte[] ZERO_SIZE_ARRAY = new byte[0];

  static {
    encoderDecoderMap.put(String.class, new StringEncoderDecoder());
  }

  // we can also simply use one buffer
  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private final Queue<ExpandableArrayBuffer> prefixKeyBuffers;
  private int keyLength;

  //  static Map<Class<?>, Function<Object, >>
  private final long columnFamilyPrefix;
  private final Class<String> valueClass;

  NewColumnFamilyContext(final long columnFamilyPrefix) {
    this.columnFamilyPrefix = columnFamilyPrefix;
    prefixKeyBuffers = new ArrayDeque<>();
    prefixKeyBuffers.add(new ExpandableArrayBuffer());
    prefixKeyBuffers.add(new ExpandableArrayBuffer());
    valueClass = String.class;
  }

  public void writeKey(final KeyType key) {
    keyLength = 0;
    keyBuffer.putLong(0, columnFamilyPrefix, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
    keyLength += Long.BYTES;

    keyLength = encoderDecoderMap.get(key.getClass()).write(keyBuffer, keyLength, key);
    //
    //    key.write(keyBuffer, Long.BYTES);
    //    keyLength += key.getLength();
  }

  public int getKeyLength() {
    return keyLength;
  }

  public byte[] getKeyBufferArray() {
    return keyBuffer.byteArray();
  }

  public int writeValue(final ValueType value) {
    return encoderDecoderMap.get(value.getClass()).write(valueBuffer, 0, value);

    //    value.write(valueBuffer, 0);
  }

  public byte[] getValueBufferArray() {
    return valueBuffer.byteArray();
  }

  public void wrapKeyView(final byte[] key) {
    if (key != null) {
      // wrap without the column family key
      keyViewBuffer.wrap(key, Long.BYTES, key.length - Long.BYTES);
    } else {
      keyViewBuffer.wrap(ZERO_SIZE_ARRAY);
    }
  }

  public DirectBuffer getKeyView() {
    return isKeyViewEmpty() ? null : keyViewBuffer;
  }

  public boolean isKeyViewEmpty() {
    return keyViewBuffer.capacity() == ZERO_SIZE_ARRAY.length;
  }

  public void wrapValueView(final byte[] value) {
    if (value != null) {
      valueViewBuffer.wrap(value);
    } else {
      valueViewBuffer.wrap(ZERO_SIZE_ARRAY);
    }
  }

  public DirectBuffer getValueView() {
    return isValueViewEmpty() ? null : valueViewBuffer;
  }

  public boolean isValueViewEmpty() {
    return valueViewBuffer.capacity() == ZERO_SIZE_ARRAY.length;
  }

  public void withPrefixKey(final DbKey key, final ObjIntConsumer<byte[]> prefixKeyConsumer) {
    if (prefixKeyBuffers.peek() == null) {
      throw new IllegalStateException(
          "Currently nested prefix iterations are not supported! This will cause unexpected behavior.");
    }

    final ExpandableArrayBuffer prefixKeyBuffer = prefixKeyBuffers.remove();
    try {
      prefixKeyBuffer.putLong(0, columnFamilyPrefix, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
      key.write(prefixKeyBuffer, Long.BYTES);
      final int prefixLength = Long.BYTES + key.getLength();

      prefixKeyConsumer.accept(prefixKeyBuffer.byteArray(), prefixLength);
    } finally {
      prefixKeyBuffers.add(prefixKeyBuffer);
    }
  }

  ByteBuffer keyWithColumnFamily(final DbKey key) {
    final var bytes = ByteBuffer.allocate(Long.BYTES + key.getLength());
    final var buffer = new UnsafeBuffer(bytes);

    buffer.putLong(0, columnFamilyPrefix, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
    key.write(buffer, Long.BYTES);
    return bytes;
  }

  public ValueType getValue() {
    return (ValueType)
        encoderDecoderMap.get(valueClass).read(valueViewBuffer, 0, valueViewBuffer.capacity());
  }

  public static class StringEncoderDecoder implements TypeEncoderDecoder<String> {
    @Override
    public int write(final MutableDirectBuffer buffer, int offset, final String typeObject) {

      final byte[] bytes = typeObject.getBytes();
      final int length = bytes.length;
      buffer.putInt(offset, length, ZB_DB_BYTE_ORDER);
      offset += Integer.BYTES;

      buffer.putBytes(offset, bytes, 0, length);
      return offset + length;
    }

    @Override
    public String read(final DirectBuffer buffer, int offset, final int length) {

      final int stringLen = buffer.getInt(offset, ZB_DB_BYTE_ORDER);
      offset += Integer.BYTES;

      final byte[] b = new byte[stringLen];
      buffer.getBytes(offset, b);
      return new String(b);
    }
  }

  public interface TypeEncoderDecoder<Type> {

    int write(final MutableDirectBuffer buffer, int offset, final Type typeObject);

    Type read(final DirectBuffer buffer, int offset, final int length);
  }
}
