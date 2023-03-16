/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.util;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

public class ExampleEnginePerfTest {

  @Rule public final EngineRule engine = EngineRule.singlePartition();

  @Test
  public void shouldRunInTime() {
    // given
    final var model =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("task", t -> t.zeebeJobType("t"))
            .endEvent()
            .done();
    engine.deployment().withXmlResource(model).deploy();

    final var start = System.currentTimeMillis();
    final long piKey = engine.processInstance().ofBpmnProcessId("process").withResult().create();

    // when
    final var jobKey =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withType("t")
            .withProcessInstanceKey(piKey)
            .getFirst()
            .getKey();
    engine.job().withKey(jobKey).withType("t").complete();
    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(piKey)
        .filter(
            r ->
                r.getIntent() == ProcessInstanceIntent.ELEMENT_COMPLETED
                    && r.getKey() == r.getValue().getProcessInstanceKey())
        .limit(1)
        .getFirst();
    final var end = System.currentTimeMillis();

    // then
    final var diff = end - start;
    Assertions.assertThat(Duration.ofMillis(diff)).isLessThan(Duration.ofMillis(100));
  }
}
