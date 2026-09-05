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
package org.apache.arrow.vector.ipc.message;

import java.util.Objects;
import java.util.Optional;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.types.MetadataVersion;

/**
 * IPC write options.
 *
 * <p>Body compression applies only to record and dictionary batch bodies; schema and metadata-only
 * messages are never compressed. Compressed codecs are resolved via {@code
 * CompressionCodec.Factory}, so applications must include {@code arrow-compression} on the runtime
 * classpath or module path to use codecs such as LZ4 or ZSTD.
 */
public class IpcOption {

  // Write the pre-0.15.0 encapsulated IPC message format
  // consisting of a 4-byte prefix instead of 8 byte
  public final boolean write_legacy_ipc_format;

  // The metadata version. Defaults to V5.
  public final MetadataVersion metadataVersion;

  // The IPC body compression codec. Defaults to no compression.
  public final CompressionUtil.CodecType codecType;

  // Optional compression level for codecs that support it.
  public final Optional<Integer> compressionLevel;

  /** Create default IPC write options with V5 metadata and no body compression. */
  public IpcOption() {
    this(
        false,
        MetadataVersion.DEFAULT,
        CompressionUtil.CodecType.NO_COMPRESSION,
        Optional.empty());
  }

  /**
   * Create IPC write options with explicit legacy framing and metadata version, but no body
   * compression.
   */
  public IpcOption(boolean writeLegacyIpcFormat, MetadataVersion metadataVersion) {
    this(
        writeLegacyIpcFormat,
        metadataVersion,
        CompressionUtil.CodecType.NO_COMPRESSION,
        Optional.empty());
  }

  /**
   * Create IPC write options with explicit legacy framing, metadata version, and body compression.
   */
  public IpcOption(
      boolean writeLegacyIpcFormat,
      MetadataVersion metadataVersion,
      CompressionUtil.CodecType codecType,
      Optional<Integer> compressionLevel) {
    this.write_legacy_ipc_format = writeLegacyIpcFormat;
    this.metadataVersion = Objects.requireNonNull(metadataVersion, "metadataVersion");
    this.codecType = Objects.requireNonNull(codecType, "codecType");
    this.compressionLevel = Objects.requireNonNull(compressionLevel, "compressionLevel");
  }

  /** Return a copy of these options with record and dictionary batch body compression enabled. */
  public IpcOption withBodyCompression(CompressionUtil.CodecType codecType) {
    return new IpcOption(write_legacy_ipc_format, metadataVersion, codecType, Optional.empty());
  }

  /**
   * Return a copy of these options with record and dictionary batch body compression enabled and an
   * explicit compression level.
   */
  public IpcOption withBodyCompression(
      CompressionUtil.CodecType codecType, int compressionLevel) {
    return new IpcOption(
        write_legacy_ipc_format,
        metadataVersion,
        codecType,
        Optional.of(compressionLevel));
  }

  public static final IpcOption DEFAULT = new IpcOption();
}
