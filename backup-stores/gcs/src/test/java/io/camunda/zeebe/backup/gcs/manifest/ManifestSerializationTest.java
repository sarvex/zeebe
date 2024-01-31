/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.gcs.manifest;

import static io.camunda.zeebe.backup.common.Manifest.StatusCode.COMPLETED;
import static io.camunda.zeebe.backup.common.Manifest.StatusCode.FAILED;
import static io.camunda.zeebe.backup.common.Manifest.StatusCode.IN_PROGRESS;
import static io.camunda.zeebe.backup.gcs.ManifestManager.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.camunda.zeebe.backup.common.BackupDescriptorImpl;
import io.camunda.zeebe.backup.common.BackupIdentifierImpl;
import io.camunda.zeebe.backup.common.BackupImpl;
import io.camunda.zeebe.backup.common.BackupStoreException.InvalidPersistedManifestState;
import io.camunda.zeebe.backup.common.FileSet;
import io.camunda.zeebe.backup.common.FileSet.NamedFile;
import io.camunda.zeebe.backup.common.Manifest;
import io.camunda.zeebe.backup.common.ManifestImpl;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class ManifestSerializationTest {
  @Test
  void shouldDeserialize() throws JsonProcessingException {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "IN_PROGRESS",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00"
        }
        """;

    // when
    final var manifest = MAPPER.readValue(json, Manifest.class);

    // then
    final BackupIdentifierImpl id = manifest.id();
    assertThat(id).isNotNull();
    assertThat(id.nodeId()).isEqualTo(1);
    assertThat(manifest.id().partitionId()).isEqualTo(2);
    assertThat(manifest.id().checkpointId()).isEqualTo(43);

    final BackupDescriptorImpl descriptor = manifest.descriptor();
    assertThat(descriptor.brokerVersion()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(descriptor.checkpointPosition()).isEqualTo(2345234L);
    assertThat(descriptor.numberOfPartitions()).isEqualTo(3);
    assertThat(descriptor.snapshotId()).isNotPresent();

    assertThat(manifest.statusCode()).isEqualTo(IN_PROGRESS);
    assertThat(manifest.createdAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(manifest.modifiedAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
  }

  @Test
  void shouldSerialize() throws JsonProcessingException {
    // given
    final var manifest =
        new ManifestImpl(
            new BackupIdentifierImpl(1, 2, 43),
            new BackupDescriptorImpl(Optional.empty(), 2345234L, 3, "1.2.0-SNAPSHOT"),
            IN_PROGRESS,
            null,
            null,
            Instant.ofEpochMilli(1678790708000L),
            Instant.ofEpochMilli(1678790708000L));
    final var expectedJsonString =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "IN_PROGRESS",
          "createdAt": "2023-03-14T10:45:08Z",
          "modifiedAt": "2023-03-14T10:45:08Z"
        }
        """;

    // when
    final String actualJsonString = MAPPER.writeValueAsString(manifest);

    // then

    final JsonNode actualJson = MAPPER.readTree(actualJsonString);
    final JsonNode expectedJson = MAPPER.readTree(expectedJsonString);

    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void shouldSerializeFailedManifest() throws JsonProcessingException {
    // given
    final var created =
        Manifest.createInProgress(
            new BackupImpl(
                new BackupIdentifierImpl(1, 2, 43),
                new BackupDescriptorImpl(Optional.empty(), 2345234L, 3, "1.2.0-SNAPSHOT"),
                null,
                null));
    final var failed = created.fail("expected failure reason");
    final var expectedJsonString =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "FAILED",
          "snapshot": { "files": [] },
          "segments": { "files": [] },
          "createdAt": "2023-03-14T10:45:08Z",
          "modifiedAt": "2023-03-14T10:45:08Z",
          "failureReason": "expected failure reason"
        }
        """;

    // when
    final String actualJsonString = MAPPER.writeValueAsString(failed);

    // then
    final JsonNode actualJson = MAPPER.readTree(actualJsonString);
    final JsonNode expectedJson = MAPPER.readTree(expectedJsonString);

    // we exclude createdAt and modifiedAt from the assertion, due to using Instant (time)
    // which is not deterministic in tests
    assertThat(actualJson.get("statusCode")).isEqualTo(expectedJson.get("statusCode"));
    assertThat(actualJson.get("id")).isEqualTo(expectedJson.get("id"));
    assertThat(actualJson.get("descriptor")).isEqualTo(expectedJson.get("descriptor"));
    assertThat(actualJson.get("snapshot")).isEqualTo(expectedJson.get("snapshot"));
    assertThat(actualJson.get("segments")).isEqualTo(expectedJson.get("segments"));
    assertThat(actualJson.get("failureReason")).isEqualTo(expectedJson.get("failureReason"));

    assertThat(actualJson.fieldNames())
        .toIterable()
        .containsExactlyElementsOf(expectedJson::fieldNames);
  }

  @Test
  void shouldFailToDeserializeFailedManifestWithWrongStatusCode() {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "IN_PROGRESS",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00",
          "failureReason": "expected failure"
        }
        """;

    // when expect thrown
    assertThatThrownBy(() -> MAPPER.readValue(json, ManifestImpl.class))
        .hasRootCauseInstanceOf(InvalidPersistedManifestState.class)
        .hasMessageContaining(
            "Expected a failed Manifest to set failureReason 'expected failure', but was in state 'IN_PROGRESS'");
  }

  @Test
  void shouldDeserializeFailedManifest() throws JsonProcessingException {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "FAILED",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00",
          "failureReason": "expected failure"
        }
        """;

    // when
    final var manifest = MAPPER.readValue(json, Manifest.class).asFailed();

    // then
    final BackupIdentifierImpl id = manifest.id();
    assertThat(id).isNotNull();
    assertThat(id.nodeId()).isEqualTo(1);
    assertThat(manifest.id().partitionId()).isEqualTo(2);
    assertThat(manifest.id().checkpointId()).isEqualTo(43);

    final BackupDescriptorImpl descriptor = manifest.descriptor();
    assertThat(descriptor.brokerVersion()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(descriptor.checkpointPosition()).isEqualTo(2345234L);
    assertThat(descriptor.numberOfPartitions()).isEqualTo(3);
    assertThat(descriptor.snapshotId()).isNotPresent();

    assertThat(manifest.statusCode()).isEqualTo(FAILED);
    assertThat(manifest.createdAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(manifest.modifiedAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(manifest.failureReason()).isEqualTo("expected failure");
  }

  @Test
  void shouldDeserializeInProgressManifest() throws JsonProcessingException {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "IN_PROGRESS",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00"
        }
        """;

    // when
    final var manifest = MAPPER.readValue(json, Manifest.class);

    // then
    final BackupIdentifierImpl id = manifest.id();
    assertThat(id).isNotNull();
    assertThat(id.nodeId()).isEqualTo(1);
    assertThat(manifest.id().partitionId()).isEqualTo(2);
    assertThat(manifest.id().checkpointId()).isEqualTo(43);

    final BackupDescriptorImpl descriptor = manifest.descriptor();
    assertThat(descriptor.brokerVersion()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(descriptor.checkpointPosition()).isEqualTo(2345234L);
    assertThat(descriptor.numberOfPartitions()).isEqualTo(3);
    assertThat(descriptor.snapshotId()).isNotPresent();

    assertThat(manifest.statusCode()).isEqualTo(IN_PROGRESS);
    assertThat(manifest.createdAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(manifest.modifiedAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
  }

  @Test
  void shouldDeserializeInProgressAndComplete() throws JsonProcessingException {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "IN_PROGRESS",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00"
        }
        """;
    final var manifest = MAPPER.readValue(json, ManifestImpl.class);

    // when
    final var complete = manifest.asInProgress().complete();

    // then
    final BackupIdentifierImpl id = complete.id();
    assertThat(id).isNotNull();
    assertThat(id.nodeId()).isEqualTo(1);
    assertThat(complete.id().partitionId()).isEqualTo(2);
    assertThat(complete.id().checkpointId()).isEqualTo(43);

    final BackupDescriptorImpl descriptor = complete.descriptor();
    assertThat(descriptor.brokerVersion()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(descriptor.checkpointPosition()).isEqualTo(2345234L);
    assertThat(descriptor.numberOfPartitions()).isEqualTo(3);
    assertThat(descriptor.snapshotId()).isNotPresent();

    assertThat(complete.statusCode()).isEqualTo(COMPLETED);
    assertThat(complete.createdAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(complete.modifiedAt()).isAfter(complete.createdAt());
  }

  @Test
  void shouldDeserializeCompletedManifest() throws JsonProcessingException {
    // given
    final var json =
        """
        {
          "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
          "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT"},
          "statusCode": "COMPLETED",
          "createdAt": "2023-03-14T10:45:08+00:00",
          "modifiedAt": "2023-03-14T10:45:08+00:00"
        }
        """;

    // when
    final var manifest = MAPPER.readValue(json, Manifest.class);

    // then
    final BackupIdentifierImpl id = manifest.id();
    assertThat(id).isNotNull();
    assertThat(id.nodeId()).isEqualTo(1);
    assertThat(manifest.id().partitionId()).isEqualTo(2);
    assertThat(manifest.id().checkpointId()).isEqualTo(43);

    final BackupDescriptorImpl descriptor = manifest.descriptor();
    assertThat(descriptor.brokerVersion()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(descriptor.checkpointPosition()).isEqualTo(2345234L);
    assertThat(descriptor.numberOfPartitions()).isEqualTo(3);
    assertThat(descriptor.snapshotId()).isNotPresent();

    assertThat(manifest.statusCode()).isEqualTo(COMPLETED);
    assertThat(manifest.createdAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
    assertThat(manifest.modifiedAt()).isEqualTo(Instant.ofEpochMilli(1678790708000L));
  }

  @Test
  void shouldSerializeFileSets() throws JsonProcessingException {
    // given
    final var manifest =
        new ManifestImpl(
            new BackupIdentifierImpl(1, 2, 43),
            new BackupDescriptorImpl(Optional.empty(), 2345234L, 3, "1.2.0-SNAPSHOT"),
            IN_PROGRESS,
            new FileSet(List.of(new NamedFile("snapshotFile1"), new NamedFile("snapshotFile2"))),
            new FileSet(List.of(new NamedFile("segmentFile1"))),
            Instant.ofEpochMilli(1678790708000L),
            Instant.ofEpochMilli(1678790708000L));
    final var expectedJsonString =
        // language=json
        """
          {
            "id": { "nodeId": 1, "partitionId": 2, "checkpointId": 43 },
            "descriptor": { "checkpointPosition": 2345234, "numberOfPartitions": 3, "brokerVersion": "1.2.0-SNAPSHOT" },
            "statusCode": "IN_PROGRESS",
            "snapshot": { "files": [ { "name": "snapshotFile1" }, { "name": "snapshotFile2" } ] },
            "segments": { "files": [ { "name": "segmentFile1" } ] },
            "createdAt": "2023-03-14T10:45:08Z",
            "modifiedAt": "2023-03-14T10:45:08Z"
          }
          """;

    // when
    final var actualJsonString = MAPPER.writeValueAsString(manifest);

    // then
    final JsonNode actualJson = MAPPER.readTree(actualJsonString);
    final JsonNode expectedJson = MAPPER.readTree(expectedJsonString);
    assertThat(actualJson).isEqualTo(expectedJson);
  }
}
