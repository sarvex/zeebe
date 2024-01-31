/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.common;

import static io.camunda.zeebe.backup.common.Manifest.StatusCode.COMPLETED;
import static io.camunda.zeebe.backup.common.Manifest.StatusCode.FAILED;
import static io.camunda.zeebe.backup.common.Manifest.StatusCode.IN_PROGRESS;

import io.camunda.zeebe.backup.common.BackupStoreException.InvalidPersistedManifestState;
import io.camunda.zeebe.backup.common.BackupStoreException.UnexpectedManifestState;
import java.time.Instant;

public record ManifestImpl(
    BackupIdentifierImpl id,
    BackupDescriptorImpl descriptor,
    StatusCode statusCode,
    FileSet snapshot,
    FileSet segments,
    Instant createdAt,
    Instant modifiedAt,
    String failureReason)
    implements Manifest.InProgressManifest, Manifest.CompletedManifest, Manifest.FailedManifest {

  public static final String ERROR_MSG_WRONG_STATE =
      "Expected a failed Manifest to set failureReason '%s', but was in state '%s'.";

  public ManifestImpl {
    if (failureReason != null && statusCode != FAILED) {
      throw new InvalidPersistedManifestState(
          ERROR_MSG_WRONG_STATE.formatted(failureReason, statusCode));
    }
  }

  public ManifestImpl(
      final BackupIdentifierImpl id,
      final BackupDescriptorImpl descriptor,
      final StatusCode statusCode,
      final FileSet snapshot,
      final FileSet segments,
      final Instant createdAt,
      final Instant modifiedAt) {
    this(id, descriptor, statusCode, snapshot, segments, createdAt, modifiedAt, null);
  }

  @Override
  public CompletedManifest complete() {
    return new ManifestImpl(
        id, descriptor, COMPLETED, snapshot, segments, createdAt, Instant.now());
  }

  @Override
  public FailedManifest fail(final String failureReason) {
    return new ManifestImpl(
        id, descriptor, FAILED, snapshot, segments, createdAt, Instant.now(), failureReason);
  }

  @Override
  public InProgressManifest asInProgress() {
    if (statusCode != IN_PROGRESS) {
      throw new UnexpectedManifestState(IN_PROGRESS, statusCode);
    }

    return this;
  }

  @Override
  public CompletedManifest asCompleted() {
    if (statusCode != COMPLETED) {
      throw new UnexpectedManifestState(COMPLETED, statusCode);
    }

    return this;
  }

  @Override
  public FailedManifest asFailed() {
    if (statusCode != FAILED) {
      throw new UnexpectedManifestState(FAILED, statusCode);
    }

    return this;
  }
}
