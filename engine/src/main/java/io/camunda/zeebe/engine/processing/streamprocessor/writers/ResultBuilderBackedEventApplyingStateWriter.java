/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor.writers;

import io.camunda.zeebe.engine.state.EventApplier;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.ProcessingResultBuilder;
import java.util.function.Supplier;

/**
 * A state writer that uses the event applier, to alter the state for each written event.
 *
 * <p>Note that it does not write events to the stream itself, but it delegates this to the {@link
 * ProcessingResultBuilder}.
 *
 * <p>Note that it does not change the state itself, but delegates this to the {@link EventApplier}.
 */
final class ResultBuilderBackedEventApplyingStateWriter extends AbstractResultBuilderBackedWriter
    implements StateWriter {

  private final EventApplier eventApplier;

  public ResultBuilderBackedEventApplyingStateWriter(
      final Supplier<ProcessingResultBuilder> resultBuilderSupplier,
      final EventApplier eventApplier) {
    super(resultBuilderSupplier);
    this.eventApplier = eventApplier;
  }

  @Override
  public void appendFollowUpEvent(final long key, final Intent intent, final RecordValue value) {
    final int latestVersion = eventApplier.getLatestVersion(intent);
    appendFollowUpEvent(key, intent, value, latestVersion);
  }

  @Override
  public void appendFollowUpEvent(
      final long key, final Intent intent, final RecordValue value, final int recordVersion) {
    final var metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .intent(intent)
            .recordVersion(recordVersion)
            .rejectionType(RejectionType.NULL_VAL)
            .rejectionReason("");
    resultBuilder().appendRecord(key, value, metadata);
    resultBuilder().appendRecord(key, value, metadata);
    eventApplier.applyState(key, intent, value, recordVersion);
  }

  @Override
  public boolean canWriteEventOfLength(final int eventLength) {
    return resultBuilder().canWriteEventOfLength(eventLength);
  }
}
