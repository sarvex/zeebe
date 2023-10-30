/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.stream;

import static io.camunda.zeebe.util.buffer.BufferUtil.wrapString;

import io.camunda.zeebe.gateway.RequestMapper;
import io.camunda.zeebe.gateway.ResponseMapper;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ActivatedJob;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.StreamActivatedJobsRequest;
import io.camunda.zeebe.protocol.impl.stream.job.ActivatedJobImpl;
import io.camunda.zeebe.protocol.impl.stream.job.JobActivationProperties;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.transport.stream.api.ClientStreamConsumer;
import io.camunda.zeebe.transport.stream.api.ClientStreamId;
import io.camunda.zeebe.transport.stream.api.ClientStreamer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamJobsObserver
    implements StreamObserver<StreamActivatedJobsRequest>, ClientStreamConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamJobsObserver.class);

  private final ConcurrencyControl executor;
  private final ClientStreamer<JobActivationProperties> jobStreamer;
  private final ServerCallStreamObserver<ActivatedJob> clientStream;

  private ClientStreamId streamId;
  private boolean isClosed;

  public StreamJobsObserver(
      final ConcurrencyControl executor,
      final ClientStreamer<JobActivationProperties> jobStreamer,
      final ServerCallStreamObserver<ActivatedJob> clientStream) {
    this.executor = executor;
    this.jobStreamer = jobStreamer;
    this.clientStream = clientStream;
  }

  @Override
  public void onNext(final StreamActivatedJobsRequest value) {
    executor.execute(() -> doOnNext(value));
  }

  @Override
  public void onError(final Throwable t) {
    executor.run(() -> doOnError(t));
  }

  @Override
  public void onCompleted() {
    executor.run(this::doOnCompleted);
  }

  @Override
  public ActorFuture<Void> push(final DirectBuffer payload) {
    try {
      return executor.call(
          () -> {
            handlePushedJob(payload);
            return null;
          });
    } catch (final Exception e) {
      // in case the actor is closed
      clientStream.onError(e);
      return CompletableActorFuture.completedExceptionally(e);
    }
  }

  private void handlePushedJob(final DirectBuffer payload) {
    final var deserializedJob = new ActivatedJobImpl();
    deserializedJob.wrap(payload);

    final var activatedJob = ResponseMapper.toActivatedJob(deserializedJob);
    try {
      clientStream.onNext(activatedJob);
    } catch (final Exception e) {
      clientStream.onError(e);
      throw e;
    }
  }

  private void doOnError(final Throwable error) {
    isClosed = true;

    // TODO: maybe don't log all errors
    LOGGER.warn("Received error from client job stream {}", streamId, error);

    if (streamId != null) {
      jobStreamer.remove(streamId);
    }
  }

  private void doOnCompleted() {
    isClosed = true;

    if (streamId != null) {
      jobStreamer.remove(streamId);
    }
  }

  private void doOnNext(final StreamActivatedJobsRequest request) {
    if (streamId == null) {
      initializeStreamOnFirstRequest(request);
      return;
    }

    LOGGER.info("Received control message: {}", request.getAmount());
  }

  private void initializeStreamOnFirstRequest(final StreamActivatedJobsRequest value) {
    final var jobType = value.getType();
    final JobActivationProperties properties;
    try {
      properties = RequestMapper.toJobActivationProperties(value);
    } catch (final Exception e) {
      clientStream.onError(
          Status.INVALID_ARGUMENT
              .withCause(e)
              .withDescription("Failed to parse job activation properties")
              .augmentDescription("Cause: " + e.getMessage())
              .asRuntimeException());
      LangUtil.rethrowUnchecked(e);
      return;
    }

    if (jobType.isBlank()) {
      handleError(clientStream, "type", "present", "blank");
      return;
    }

    if (properties.timeout() < 1) {
      handleError(
          clientStream, "timeout", "greater than zero", Long.toString(properties.timeout()));
      return;
    }

    executor.runOnCompletion(
        jobStreamer.add(wrapString(jobType), properties, this), this::onStreamAdded);
  }

  private void onStreamAdded(final ClientStreamId streamId, final Throwable error) {
    // the only possible reason it would fail is due to the actor being closed, meaning we would be
    // shutting down or a fatal error occurred; in either case, retrying would do no good
    if (error != null) {
      LOGGER.warn("Failed to register new job stream", error);
      clientStream.onError(
          Status.UNAVAILABLE
              .withDescription("Failed to register new job stream")
              .withCause(error)
              .augmentDescription("Cause: " + error.getMessage())
              .asRuntimeException());
      return;
    }

    if (isClosed) {
      jobStreamer.remove(streamId);
      return;
    }

    this.streamId = streamId;
  }

  private void handleError(
      final ServerCallStreamObserver<GatewayOuterClass.ActivatedJob> responseObserver,
      final String field,
      final String expectation,
      final String actual) {
    final var format = "Expected to stream activated jobs with %s to be %s, but it was %s";
    final var errorMessage = format.formatted(field, expectation, actual);

    isClosed = true;
    responseObserver.onError(
        new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(errorMessage)));
  }
}
