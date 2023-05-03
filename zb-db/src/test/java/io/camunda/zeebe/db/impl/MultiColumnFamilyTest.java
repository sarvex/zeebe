/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.ZeebeDbFactory;
import io.camunda.zeebe.db.ZeebeDbTransaction;
import java.io.File;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class MultiColumnFamilyTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final ZeebeDbFactory<ColumnFamilies> dbFactory =
      DefaultZeebeDbFactory.getDefaultFactory();
  private ZeebeDb<ColumnFamilies> zeebeDb;
  private ColumnFamily<DbCompositeKey<DbLong, DbString>, DbString> compositeCF;
  private DbLong compositeCFFirstKey;
  private DbString compositeCFSecondKey;
  private DbCompositeKey<DbLong, DbString> compositeKey;
  private DbString value;
  private DbLong defaultKey;
  private DbLong defaultValue;
  private ColumnFamily<DbLong, DbLong> defaultCF;
  private DbString otherKey;
  private DbString otherValue;
  private ColumnFamily<DbString, DbString> otherCF;
  private TransactionContext transactionContext;

  @Before
  public void setup() throws Exception {

    final File pathName = temporaryFolder.newFolder();
    zeebeDb = dbFactory.createDb(pathName);

    transactionContext = zeebeDb.createContext();

    compositeCFFirstKey = new DbLong();
    compositeCFSecondKey = new DbString();
    compositeKey = new DbCompositeKey<>(compositeCFFirstKey, compositeCFSecondKey);
    value = new DbString();

    compositeCF =
        zeebeDb.createColumnFamily(
            ColumnFamilies.COMPOSITE, transactionContext, compositeKey, value);

    defaultKey = new DbLong();
    defaultValue = new DbLong();
    defaultCF =
        zeebeDb.createColumnFamily(
            ColumnFamilies.DEFAULT, transactionContext, defaultKey, defaultValue);

    otherKey = new DbString();
    otherValue = new DbString();
    otherCF =
        zeebeDb.createColumnFamily(ColumnFamilies.OTHER, transactionContext, otherKey, otherValue);
  }

  @Test
  public void shouldLoopWhileEqualPrefixEndOfCF() throws Exception {
    // given
    upsertInDefaultCF(10, 1);
    upsertInDefaultCF(11, 2);
    upsertInCompositeCF(11, "foo", "baring");
    upsertInCompositeCF(11, "foo2", "different value");
    upsertInCompositeCF(12, "hello", "world");
    upsertInCompositeCF(12, "this is the one", "as you know");
    upsertInCompositeCF(13, "another", "string");
    upsertInCompositeCF(14, "might", "be good");
    upsertInOtherCF("10", "1");
    upsertInOtherCF("10", "2");

    // when
    final var keyValues = new HashMap<Long, Long>();
    final ZeebeDbTransaction currentTransaction = transactionContext.getCurrentTransaction();
    currentTransaction.run(
        () -> {
          upsertInCompositeCF(114, "12", "be good");
          upsertInOtherCF("10", "1121");
        });

    // we don't commit, so it is still on going
    //    currentTransaction.commit();
    defaultCF.forEach((key, value) -> keyValues.put(key.getValue(), value.getValue()));

    // then
    assertThat(keyValues).containsOnlyKeys(10L, 11L).containsValues(1L, 2L);
  }

  private void upsertInDefaultCF(final long key, final long value) {
    defaultKey.wrapLong(key);
    defaultValue.wrapLong(value);
    defaultCF.upsert(defaultKey, defaultValue);
  }

  private void upsertInOtherCF(final String key, final String value) {
    otherKey.wrapString(key);
    otherValue.wrapString(value);
    otherCF.upsert(otherKey, otherValue);
  }

  private void upsertInCompositeCF(
      final long firstKey, final String secondKey, final String value) {
    compositeCFFirstKey.wrapLong(firstKey);
    compositeCFSecondKey.wrapString(secondKey);

    this.value.wrapString(value);
    compositeCF.upsert(compositeKey, this.value);
  }

  enum ColumnFamilies {
    DEFAULT,
    COMPOSITE,
    OTHER,
  }
}
