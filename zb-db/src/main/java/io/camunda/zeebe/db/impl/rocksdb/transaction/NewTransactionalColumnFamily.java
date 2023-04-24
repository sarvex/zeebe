/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import io.camunda.zeebe.db.ConsistencyChecksSettings;
import io.camunda.zeebe.db.ContainsForeignKeys;
import io.camunda.zeebe.db.DbKey;
import io.camunda.zeebe.db.NewColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

/**
 * Some code conventions that we should follow here:
 *
 * <ul>
 *   <li>Public methods ensure that a transaction is open by using {@link
 *       NewTransactionalColumnFamily#ensureInOpenTransaction}, private methods can assume that a
 *       transaction is already open and don't need to call ensureInOpenTransaction.
 *   <li>Iteration is implemented in terms of {@link NewTransactionalColumnFamily#forEachInPrefix}
 *       to depend difficult to follow call chains between the different public methods such as
 *       {@link NewTransactionalColumnFamily#forEach(Consumer)} and {@link
 *       NewTransactionalColumnFamily#whileEqualPrefix(DbKey, BiConsumer)}
 * </ul>
 */
class NewTransactionalColumnFamily<
        ColumnFamilyNames extends Enum<ColumnFamilyNames>, KeyType, ValueType>
    implements NewColumnFamily<KeyType, ValueType> {

  private final ZeebeTransactionDb<ColumnFamilyNames> transactionDb;
  private final ConsistencyChecksSettings consistencyChecksSettings;
  private final ColumnFamilyNames columnFamily;
  private final TransactionContext context;

  private final NewColumnFamilyContext<KeyType, ValueType> columnFamilyContext;

  private final ForeignKeyChecker foreignKeyChecker;

  NewTransactionalColumnFamily(
      final ZeebeTransactionDb<ColumnFamilyNames> transactionDb,
      final ConsistencyChecksSettings consistencyChecksSettings,
      final ColumnFamilyNames columnFamily,
      final TransactionContext context) {
    this.transactionDb = transactionDb;
    this.consistencyChecksSettings = consistencyChecksSettings;
    this.columnFamily = columnFamily;
    this.context = context;
    columnFamilyContext = new NewColumnFamilyContext<>(columnFamily.ordinal());
    foreignKeyChecker = new ForeignKeyChecker(transactionDb, consistencyChecksSettings);
  }

  @Override
  public void insert(final KeyType key, final ValueType value) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          final int valueLen = columnFamilyContext.writeValue(value);

          assertForeignKeysExist(transaction, key, value);
          transaction.put(
              transactionDb.getDefaultNativeHandle(),
              columnFamilyContext.getKeyBufferArray(),
              columnFamilyContext.getKeyLength(),
              columnFamilyContext.getValueBufferArray(),
              valueLen);
        });
  }

  @Override
  public ValueType get(final KeyType key) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          final byte[] value =
              transaction.get(
                  transactionDb.getDefaultNativeHandle(),
                  transactionDb.getReadOptionsNativeHandle(),
                  columnFamilyContext.getKeyBufferArray(),
                  columnFamilyContext.getKeyLength());
          columnFamilyContext.wrapValueView(value);
        });
    final var valueBuffer = columnFamilyContext.getValueView();

    if (valueBuffer != null) {
      return columnFamilyContext.getValue();
      //      valueInstance.wrap(valueBuffer, 0, valueBuffer.capacity());
      //      return valueInstance;
    }
    return null;
  }

  //  @Override
  //  public void forEach(final Consumer<ValueType> consumer) {
  //    ensureInOpenTransaction(
  //        transaction ->
  //            forEachInPrefix(
  //                new DbNullKey(),
  //                (k, v) -> {
  //                  consumer.accept(v);
  //                  return true;
  //                }));
  //  }

  private void assertForeignKeysExist(final ZeebeTransaction transaction, final Object... keys)
      throws Exception {
    if (!consistencyChecksSettings.enableForeignKeyChecks()) {
      return;
    }
    for (final var key : keys) {
      if (key instanceof ContainsForeignKeys containsForeignKey) {
        foreignKeyChecker.assertExists(transaction, containsForeignKey);
      }
    }
  }

  private void ensureInOpenTransaction(final TransactionConsumer operation) {
    context.runInTransaction(
        () -> operation.run((ZeebeTransaction) context.getCurrentTransaction()));
  }

  RocksIterator newIterator(final TransactionContext context, final ReadOptions options) {
    final var currentTransaction = (ZeebeTransaction) context.getCurrentTransaction();
    return currentTransaction.newIterator(options, transactionDb.getDefaultHandle());
  }
  //
  //  /**
  //   * This is the preferred method to implement methods that iterate over a column family.
  //   *
  //   * @param prefix of all keys that are iterated over.
  //   * @param visitor called for all kv pairs where the key matches the given prefix. The visitor
  // can
  //   *     indicate whether iteration should continue or not, see {@link KeyValuePairVisitor}.
  //   */
  //  private void forEachInPrefix(
  //      final DbKey prefix, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
  //    forEachInPrefix(prefix, prefix, visitor);
  //  }
  //  /**
  //   * This is the preferred method to implement methods that iterate over a column family.
  //   *
  //   * @param startAt seek to this key before starting iteration. If null, seek to {@code prefix}
  //   *     instead.
  //   * @param prefix of all keys that are iterated over.
  //   * @param visitor called for all kv pairs where the key matches the given prefix. The visitor
  // can
  //   *     indicate whether iteration should continue or not, see {@link KeyValuePairVisitor}.
  //   */
  //  private void forEachInPrefix(
  //      final DbKey startAt,
  //      final DbKey prefix,
  //      final KeyValuePairVisitor<KeyType, ValueType> visitor) {
  //    final var seekTarget = Objects.requireNonNullElse(startAt, prefix);
  //    Objects.requireNonNull(prefix);
  //    Objects.requireNonNull(visitor);
  //
  //    /*
  //     * NOTE: it doesn't seem possible in Java RocksDB to set a flexible prefix extractor on
  //     * iterators at the moment, so using prefixes seem to be mostly related to skipping files
  // that
  //     * do not contain keys with the given prefix (which is useful anyway), but it will still
  // iterate
  //     * over all keys contained in those files, so we still need to make sure the key actually
  //     * matches the prefix.
  //     *
  //     * <p>While iterating over subsequent keys we have to validate it.
  //     */
  //    columnFamilyContext.withPrefixKey(
  //        prefix,
  //        (prefixKey, prefixLength) -> {
  //          try (final RocksIterator iterator =
  //              newIterator(context, transactionDb.getPrefixReadOptions())) {
  //
  //            boolean shouldVisitNext = true;
  //
  //            for (iterator.seek(columnFamilyContext.keyWithColumnFamily(seekTarget));
  //                iterator.isValid() && shouldVisitNext;
  //                iterator.next()) {
  //              final byte[] keyBytes = iterator.key();
  //              if (!startsWith(prefixKey, 0, prefixLength, keyBytes, 0, keyBytes.length)) {
  //                break;
  //              }
  //
  //              shouldVisitNext = visit(keyInstance, valueInstance, visitor, iterator);
  //            }
  //          }
  //        });
  //  }
  //
  //  private boolean visit(
  //      final KeyType keyInstance,
  //      final ValueType valueInstance,
  //      final KeyValuePairVisitor<KeyType, ValueType> iteratorConsumer,
  //      final RocksIterator iterator) {
  //    final var keyBytes = iterator.key();
  //
  //    columnFamilyContext.wrapKeyView(keyBytes);
  //    columnFamilyContext.wrapValueView(iterator.value());
  //
  //    final DirectBuffer keyViewBuffer = columnFamilyContext.getKeyView();
  //    keyInstance.wrap(keyViewBuffer, 0, keyViewBuffer.capacity());
  //    final DirectBuffer valueViewBuffer = columnFamilyContext.getValueView();
  //    valueInstance.wrap(valueViewBuffer, 0, valueViewBuffer.capacity());
  //
  //    return iteratorConsumer.visit(keyInstance, valueInstance);
  //  }
}
