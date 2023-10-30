/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.client.impl.command;

import io.camunda.zeebe.client.api.JsonMapper;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.impl.response.ActivatedJobImpl;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.StreamActivatedJobsRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.function.Consumer;

public class ClientJobStreamObserver
    implements ClientResponseObserver<StreamActivatedJobsRequest, GatewayOuterClass.ActivatedJob>,
        AutoCloseable {

  private final Consumer<Throwable> onErrorListener;
  private final JsonMapper jsonMapper;
  private final Consumer<ActivatedJob> jobConsumer;
  private ClientCallStreamObserver<StreamActivatedJobsRequest> requestStream;

  public ClientJobStreamObserver(
      final Consumer<Throwable> onErrorListener,
      final JsonMapper jsonMapper,
      final Consumer<ActivatedJob> jobConsumer) {
    this.onErrorListener = onErrorListener;
    this.jsonMapper = jsonMapper;
    this.jobConsumer = jobConsumer;
  }

  @Override
  public void beforeStart(
      final ClientCallStreamObserver<StreamActivatedJobsRequest> requestStream) {
    this.requestStream = requestStream;
  }

  @Override
  public void onNext(final GatewayOuterClass.ActivatedJob value) {
    try {
      final ActivatedJobImpl mappedJob = new ActivatedJobImpl(jsonMapper, value);
      jobConsumer.accept(mappedJob);
    } catch (final Exception exception) {
      onError(exception);
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void onError(final Throwable t) {
    onErrorListener.accept(t);
  }

  @Override
  public void onCompleted() {}

  @Override
  public void close() throws Exception {
    // todo close stream?!
  }
}
