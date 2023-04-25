/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

//
// public class CompositeEncoderDecoder implements TypeEncoderDecoder<StringStringComposite> {
//
//  private final Map<Class, TypeEncoderDecoder> encoderDecoderMap;
//
//  public CompositeEncoderDecoder(final Map<Class, TypeEncoderDecoder> encoderDecoderMap) {
//    this.encoderDecoderMap = encoderDecoderMap;
//  }
//
//  @Override
//  public int write(
//      final MutableDirectBuffer buffer,
//      final int offset,
//      final StringStringComposite compositeKey) {
//
//    final var firstEncoder = encoderDecoderMap.get(compositeKey.firstKey());
//    final int newOffset = firstEncoder.write(buffer, offset, compositeKey.firstKey());
//
//    final var secondEncoder = encoderDecoderMap.get(compositeKey.secondKey());
//    final int newLength = secondEncoder.write(buffer, newOffset, compositeKey.secondKey());
//
//    return newLength;
//    //
//    //
//    //    final byte[] bytes = typeObject.getBytes();
//    //    final int length = bytes.length;
//    //    buffer.putInt(offset, length, ZB_DB_BYTE_ORDER);
//    //    offset += Integer.BYTES;
//    //
//    //    buffer.putBytes(offset, bytes, 0, length);
//    //    return offset + length;
//  }
//
//  @Override
//  public StringStringComposite read(final DirectBuffer buffer, final int offset, final int length)
// {
//    throw new UnsupportedOperationException();
//    //
//    //    final int stringLen = buffer.getInt(offset, ZB_DB_BYTE_ORDER);
//    //    offset += Integer.BYTES;
//    //
//    //    final byte[] b = new byte[stringLen];
//    //    buffer.getBytes(offset, b);
//    //    return new String(b);
//  }
// }
