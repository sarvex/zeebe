/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import static io.camunda.zeebe.db.impl.ZeebeDbConstants.ZB_DB_BYTE_ORDER;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class StringEncoderDecoder implements TypeEncoderDecoder<String> {
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
