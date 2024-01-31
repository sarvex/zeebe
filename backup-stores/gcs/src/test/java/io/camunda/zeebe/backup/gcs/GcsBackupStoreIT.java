/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.gcs;

import com.google.cloud.storage.BucketInfo;
import io.camunda.zeebe.backup.api.BackupStore;
import io.camunda.zeebe.backup.common.BackupStoreException.UnexpectedManifestState;
import io.camunda.zeebe.backup.gcs.util.GcsContainer;
import io.camunda.zeebe.backup.testkit.BackupStoreTestKit;
import java.nio.file.NoSuchFileException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class GcsBackupStoreIT {
  @Container private static final GcsContainer GCS = new GcsContainer();

  @Nested
  final class WithBasePath implements BackupStoreTestKit {
    private static final String BUCKET_NAME = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    private GcsBackupStore store;

    @BeforeAll
    static void createBucket() {
      final var config =
          new GcsBackupConfig.Builder()
              .withBucketName(BUCKET_NAME)
              .withHost(GCS.externalEndpoint())
              .withoutAuthentication()
              .build();
      try (final var client = GcsBackupStore.buildClient(config)) {
        client.create(BucketInfo.of(BUCKET_NAME));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @BeforeEach
    void setup() {
      store =
          new GcsBackupStore(
              new GcsBackupConfig.Builder()
                  .withBucketName(BUCKET_NAME)
                  .withBasePath(RandomStringUtils.randomAlphabetic(10).toLowerCase())
                  .withHost(GCS.externalEndpoint())
                  .withoutAuthentication()
                  .build());
    }

    @AfterEach
    void tearDown() {
      store.closeAsync().join();
    }

    @Override
    public BackupStore getStore() {
      return store;
    }

    @Override
    public Class<? extends Exception> getBackupInInvalidStateExceptionClass() {
      return UnexpectedManifestState.class;
    }

    @Override
    public Class<? extends Exception> getFileNotFoundExceptionClass() {
      return NoSuchFileException.class;
    }
  }

  @Nested
  final class WithoutBasePath implements BackupStoreTestKit {

    private GcsBackupStore store;

    @BeforeEach
    void setup() throws Exception {
      final var bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

      final var config =
          new GcsBackupConfig.Builder()
              .withBucketName(bucketName)
              .withHost(GCS.externalEndpoint())
              .withoutAuthentication()
              .build();

      try (final var client = GcsBackupStore.buildClient(config)) {
        client.create(BucketInfo.of(bucketName));
      }

      store = new GcsBackupStore(config);
    }

    @AfterEach
    void tearDown() {
      store.closeAsync().join();
    }

    @Override
    public BackupStore getStore() {
      return store;
    }

    @Override
    public Class<? extends Exception> getBackupInInvalidStateExceptionClass() {
      return UnexpectedManifestState.class;
    }

    @Override
    public Class<? extends Exception> getFileNotFoundExceptionClass() {
      return NoSuchFileException.class;
    }
  }
}
