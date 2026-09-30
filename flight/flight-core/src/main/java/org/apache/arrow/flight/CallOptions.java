/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight;

import io.grpc.stub.AbstractStub;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/** Common call options. */
public class CallOptions {
  public static CallOption timeout(long duration, TimeUnit unit) {
    return new Timeout(duration, unit);
  }

  /**
   * Advertise support for IPC body compression codecs in preference order.
   *
   * <p>Servers that do not support negotiation ignore this option and return uncompressed IPC.
   */
  public static CallOption acceptIpcCompression(CompressionUtil.CodecType... codecs) {
    if (codecs.length == 0) {
      throw new IllegalArgumentException("At least one IPC compression codec is required");
    }
    for (CompressionUtil.CodecType codec : codecs) {
      CompressionCodec.Factory.INSTANCE.createCodec(codec);
    }
    final FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert(
        FlightConstants.IPC_ACCEPT_COMPRESSION_HEADER,
        Arrays.stream(codecs).map(IpcCompression::codecName).collect(Collectors.joining(",")));
    return new HeaderCallOption(headers);
  }

  static <T extends AbstractStub<T>> T wrapStub(T stub, CallOption[] options) {
    for (CallOption option : options) {
      if (option instanceof GrpcCallOption) {
        stub = ((GrpcCallOption) option).wrapStub(stub);
      }
    }
    return stub;
  }

  private static class Timeout implements GrpcCallOption {
    long timeout;
    TimeUnit timeoutUnit;

    Timeout(long timeout, TimeUnit timeoutUnit) {
      this.timeout = timeout;
      this.timeoutUnit = timeoutUnit;
    }

    @Override
    public <T extends AbstractStub<T>> T wrapStub(T stub) {
      return stub.withDeadlineAfter(timeout, timeoutUnit);
    }
  }

  /** CallOptions specific to GRPC stubs. */
  public interface GrpcCallOption extends CallOption {
    <T extends AbstractStub<T>> T wrapStub(T stub);
  }
}
