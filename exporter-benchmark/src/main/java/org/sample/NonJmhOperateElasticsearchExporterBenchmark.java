/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.sample;

import java.time.Duration;
import java.time.Instant;

public class NonJmhOperateElasticsearchExporterBenchmark {

  public static void main(String[] args) throws Exception {

    System.out.println("Setting up benchmark");

    Duration benchmarkDuration = Duration.ofSeconds(60);

    OperateElasticsearchExporterState exporterState = new OperateElasticsearchExporterState();

    setUp(exporterState);

    ExporterBenchmark benchmark = new ExporterBenchmark();

    System.out.println("Running benchmark");

    runBenchmark(benchmarkDuration, benchmark, exporterState);

    tearDown(exporterState);

    System.out.println("Torn down benchmark");
  }

  private static void runBenchmark(
      Duration benchmarkDuration,
      ExporterBenchmark benchmark,
      OperateElasticsearchExporterState exporterState) {
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(benchmarkDuration);
    int iterations = 0;

    while (Instant.now().isBefore(endTime)) {
      benchmark.testPrototypeExporter(exporterState);
      iterations++;
    }

    System.out.println("Finished benchmark");
    System.out.printf(
        "Completed iterations per second: %s%n",
        (double) iterations / benchmarkDuration.toSeconds());
  }

  private static void tearDown(OperateElasticsearchExporterState exporterState) {
    exporterState.tearDownIteration();
    exporterState.tearDownTrial();
  }

  private static void setUp(OperateElasticsearchExporterState exporterState) throws Exception {
    exporterState.setUpTrial();
    exporterState.setUpIteration();
  }
}
