/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.client.command;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.camunda.zeebe.client.api.command.ClientStatusException;
import io.camunda.zeebe.qa.util.cluster.TestStandaloneBroker;
import io.camunda.zeebe.qa.util.junit.ZeebeIntegration;
import io.camunda.zeebe.qa.util.junit.ZeebeIntegration.TestZeebe;
import io.grpc.Status.Code;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

@ZeebeIntegration
public class EndlessLoopTest {
  @TestZeebe private final TestStandaloneBroker broker = new TestStandaloneBroker();

  private final String dmn =
      """
<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="_6f87b3aa-d613-4c41-93ad-832f021c8318" name="definitions" namespace="http://camunda.org/schema/1.0/dmn">
  <decision id="_51711_44808585-3491-42a8-87ea-3c4fcda46eff" name="">
    <decisionTable id="_07c6fdc3-7bdf-459a-a016-2ae12e474bd2">
      <input id="_f00d0717-d08a-4918-ad51-568788e92038" label="Percent Responsible (Loan Roles)">
        <inputExpression id="_0b922041-84c3-47bf-b94c-1dd423c394ae" typeRef="number">
          <text>proposedLoan.loanRoles.percentResponsible</text>
        </inputExpression>
      </input>
      <output id="_b304292d-98e5-4dde-8fbd-0de1981c97ea" label="" name="out" typeRef="string">
        <outputValues id="UnaryTests_1yu7moy">
          <text>"Approve","Decline","Review"</text>
        </outputValues>
      </output>
      <rule id="DecisionRule_1j6jzzn">
        <inputEntry id="UnaryTests_105fhm8">
          <text></text>
        </inputEntry>
        <outputEntry id="LiteralExpression_1ucr6zl">
          <text>"Approve"</text>
        </outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
""";

  @Test
  void shouldNotLoopEndlessly() {
    // given
    try (final var client = broker.newClientBuilder().build()) {
      // when
      final var result =
          client
              .newDeployResourceCommand()
              .addResourceBytes(dmn.getBytes(StandardCharsets.UTF_8), "bork.dmn")
              .requestTimeout(Duration.ofSeconds(30))
              .send();

      final var secondCommandIsCompleted =
          client.newPublishMessageCommand().messageName("foo").correlationKey("test").send().join();
      // then - expand the assert to check that it's rejected appropriately within reasonable amount
      // of time
      assertThatThrownBy(result::join)
          .isInstanceOf(ClientStatusException.class)
          .extracting("status.code")
          .isEqualTo(Code.INVALID_ARGUMENT);
    }
  }
}
