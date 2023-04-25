/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

// We need strong typed classes, which specialized for each generic in order to be able to read
// the correct values and sub-values
// java has weak generics and generic types are lost during runtime, which makes it hard to
// determine
// which type/class we want to read based on generics - Tuple<String,String> doesn't work as type
// the secondary types are even weaker and not really accesible via Class<> which means on our
// mapping
// we wouldn't find the extractor - Best way to have explicity types which model the combination
// makes easier (instead of generic magic (which not really works anyway)) and clearer
public class StringStringCompositeEncoderDecoder
    implements TypeEncoderDecoder<StringStringComposite> {

  private final StringEncoderDecoder stringEncoderDecoder = new StringEncoderDecoder();

  @Override
  public int write(
      final MutableDirectBuffer buffer,
      final int offset,
      final StringStringComposite compositeKey) {

    final int newOffset = stringEncoderDecoder.write(buffer, offset, compositeKey.firstKey());
    final int newLength = stringEncoderDecoder.write(buffer, newOffset, compositeKey.secondKey());

    return newLength;
    //
    //
    //    final byte[] bytes = typeObject.getBytes();
    //    final int length = bytes.length;
    //    buffer.putInt(offset, length, ZB_DB_BYTE_ORDER);
    //    offset += Integer.BYTES;
    //
    //    buffer.putBytes(offset, bytes, 0, length);
    //    return offset + length;
  }

  @Override
  public StringStringComposite read(final DirectBuffer buffer, final int offset, final int length) {

    final String firstString = stringEncoderDecoder.read(buffer, offset, length);
    final String secondString =
        stringEncoderDecoder.read(buffer, offset + firstString.length(), length);
    return StringStringComposite.of(firstString, secondString);
    //
    //    final int stringLen = buffer.getInt(offset, ZB_DB_BYTE_ORDER);
    //    offset += Integer.BYTES;
    //
    //    final byte[] b = new byte[stringLen];
    //    buffer.getBytes(offset, b);
    //    return new String(b);
  }
}
