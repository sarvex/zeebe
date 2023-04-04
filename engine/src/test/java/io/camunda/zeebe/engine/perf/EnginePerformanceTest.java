/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.perf;

import io.camunda.zeebe.engine.perf.EngineRule.ProcessingExporterTransistor;
import io.camunda.zeebe.engine.perf.EngineRule.ReprocessingCompletedListener;
import io.camunda.zeebe.engine.processing.EngineProcessors;
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.camunda.zeebe.engine.processing.streamprocessor.JobStreamer;
import io.camunda.zeebe.engine.state.DefaultZeebeDbFactory;
import io.camunda.zeebe.engine.util.StreamProcessingComposite;
import io.camunda.zeebe.engine.util.TestStreams;
import io.camunda.zeebe.engine.util.client.DeploymentClient;
import io.camunda.zeebe.engine.util.client.ProcessInstanceClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.scheduler.ActorScheduler;
import io.camunda.zeebe.scheduler.clock.DefaultActorClock;
import io.camunda.zeebe.stream.impl.StreamProcessorMode;
import io.camunda.zeebe.test.util.AutoCloseableRule;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.util.FeatureFlags;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.LoggerFactory;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 200, time = 1, timeUnit = TimeUnit.MILLISECONDS)
// @Fork(value = 3, jvmArgsAppend = {"-XX:+UseParallelGC", "-Xms1g", "-Xmx1g"})
@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class EnginePerformanceTest {

  // BASE
  //  Result "io.camunda.zeebe.engine.perf.EnginePerformanceTest.measureProcessExecutionTime":
  //      0.370 ±(99.9%) 0.029 ops/ms [Average]
  //      (min, avg, max) = (0.088, 0.370, 0.670), stdev = 0.125
  //  CI (99.9%): [0.340, 0.399] (assumes normal distribution)
  //
  //  # Run complete. Total time: 00:00:04
  //
  //  Benchmark                                           Mode  Cnt  Score   Error   Units
  //  EnginePerformanceTest.measureProcessExecutionTime  thrpt  200  0.370 ± 0.029  ops/ms
  //
  //  Process finished with exit code 0

  //  BIG STATE
  //  Result "io.camunda.zeebe.engine.perf.EnginePerformanceTest.measureProcessExecutionTime":
  //      0.093 ±(99.9%) 0.003 ops/ms [Average]
  //      (min, avg, max) = (0.051, 0.093, 0.117), stdev = 0.014
  //  CI (99.9%): [0.090, 0.097] (assumes normal distribution)
  //  # Run complete. Total time: 00:00:05
  //  Benchmark                                           Mode  Cnt  Score   Error   Units
  //  EnginePerformanceTest.measureProcessExecutionTime  thrpt  200  0.093 ± 0.003  ops/ms
  // fixes
  //  Result "io.camunda.zeebe.engine.perf.EnginePerformanceTest.measureProcessExecutionTime":
  //      0.417 ±(99.9%) 0.035 ops/ms [Average]
  //      (min, avg, max) = (0.081, 0.417, 0.768), stdev = 0.146
  //  CI (99.9%): [0.383, 0.452] (assumes normal distribution)
  //
  //
  //      # Run complete. Total time: 00:00:04
  //
  //  Benchmark                                           Mode  Cnt  Score   Error   Units
  //  EnginePerformanceTest.measureProcessExecutionTime  thrpt  200  0.417 ± 0.035  ops/ms
  //
  //  Process finished with exit code 0

  private StreamProcessingComposite streamProcessingComposite;
  private AutoCloseableRule autoCloseableRule;

  @Setup
  public void setup() throws Throwable {

    autoCloseableRule = new AutoCloseableRule();
    //
    //    final Path tempDirectory = Files.createTempDirectory("perf");
    final var temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();

    // scheduler
    final var builder =
        ActorScheduler.newActorScheduler()
            .setCpuBoundActorThreadCount(2)
            .setIoBoundActorThreadCount(2)
            .setActorClock(new DefaultActorClock());

    final var actorScheduler = builder.build();
    autoCloseableRule.manage(actorScheduler);
    actorScheduler.start();

    final var testStreams = new TestStreams(temporaryFolder, autoCloseableRule, actorScheduler);
    testStreams.withStreamProcessorMode(StreamProcessorMode.PROCESSING);
    testStreams.maxCommandsInBatch(100);

    testStreams.createLogStream("stream-1", 1);

    streamProcessingComposite =
        new StreamProcessingComposite(
            testStreams, 1, DefaultZeebeDbFactory.defaultFactory(), actorScheduler);

    // engine
    final DeploymentRecord deploymentRecord = new DeploymentRecord();
    final UnsafeBuffer deploymentBuffer = new UnsafeBuffer(new byte[deploymentRecord.getLength()]);
    deploymentRecord.write(deploymentBuffer, 0);
    final var interPartitionCommandSenders = new ArrayList<TestInterPartitionCommandSender>();
    final Map<Integer, ReprocessingCompletedListener> partitionReprocessingCompleteListeners =
        new Int2ObjectHashMap<>();

    final var reprocessingCompletedListener = new ReprocessingCompletedListener();
    partitionReprocessingCompleteListeners.put(1, reprocessingCompletedListener);
    final var featureFlags = FeatureFlags.createDefaultForTests();

    final var interPartitionCommandSender =
        new TestInterPartitionCommandSender(streamProcessingComposite::newLogStreamWriter);
    interPartitionCommandSenders.add(interPartitionCommandSender);
    autoCloseableRule.manage(
        streamProcessingComposite.startTypedStreamProcessor(
            1,
            (recordProcessorContext) ->
                EngineProcessors.createEngineProcessors(
                        recordProcessorContext,
                        1,
                        new SubscriptionCommandSender(1, interPartitionCommandSender),
                        interPartitionCommandSender,
                        featureFlags,
                        JobStreamer.noop())
                    .withListener(
                        new ProcessingExporterTransistor(testStreams.getLogStream("stream-1")))
                    .withListener(reprocessingCompletedListener),
            Optional.empty()));
    interPartitionCommandSenders.forEach(s -> s.initializeWriters(1));

    new DeploymentClient(streamProcessingComposite, (p) -> p.accept(1))
        .withXmlResource(
            Bpmn.createExecutableProcess("process")
                .startEvent()
                .serviceTask("task", (t) -> t.zeebeJobType("task").done())
                .endEvent()
                .done())
        .deploy();
  }

  @TearDown
  public void tearDown() {
    final long count =
        RecordingExporter.processInstanceRecords()
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withElementType(BpmnElementType.PROCESS)
            .limit(200 + 5) // iteration + warmup
            .count();

    LoggerFactory.getLogger("io.camunda.zeebe.engine.perf")
        .info("Started {} process instances", count);
    autoCloseableRule.after();
  }

  @Benchmark
  public Record<?> measureProcessExecutionTime() {
    final long piKey =
        new ProcessInstanceClient(streamProcessingComposite).ofBpmnProcessId("process").create();

    return RecordingExporter.jobRecords()
        .withIntent(JobIntent.CREATED)
        .withType("task")
        .withProcessInstanceKey(piKey)
        .getFirst();
    // TODO benchmark with completion
    //    new
    // JobClient(streamProcessingComposite).withKey(job.getKey()).withType("task").complete();
    //
    //    return RecordingExporter.processInstanceRecords()
    //        .withProcessInstanceKey(piKey)
    //        .limitToProcessInstanceCompleted()
    //        .getLast();
  }
}
