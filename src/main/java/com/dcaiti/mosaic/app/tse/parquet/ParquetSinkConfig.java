/*
 * Copyright (c) 2023 Fraunhofer FOKUS and others. All rights reserved.
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contact: mosaic@fokus.fraunhofer.de
 */

package com.dcaiti.mosaic.app.tse.parquet;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Objects;

public final class ParquetSinkConfig {
    private final String path;
    private final String filename;
    private final CompressionCodecName compressionCodec;
    private final boolean enableDictionary;

    private ParquetSinkConfig(String path, String filename, CompressionCodecName compressionCodec, boolean enableDictionary) {
        this.path = path;
        this.filename = Objects.requireNonNull(filename, "filename");
        this.compressionCodec = Objects.requireNonNull(compressionCodec, "compressionCodec");
        this.enableDictionary = enableDictionary;
    }

    public static ParquetSinkConfig of(String path, String filename) {
        return new ParquetSinkConfig(path, filename, CompressionCodecName.GZIP, true);
    }

    public ParquetSinkConfig withCompressionCodec(CompressionCodecName codec) {
        return new ParquetSinkConfig(path, filename, codec, enableDictionary);
    }

    public ParquetSinkConfig withDictionaryEncoding(boolean enabled) {
        return new ParquetSinkConfig(path, filename, compressionCodec, enabled);
    }

    public String getPath() {
        return path;
    }

    public String getFilename() {
        return filename;
    }

    public CompressionCodecName getCompressionCodec() {
        return compressionCodec;
    }

    public boolean isDictionaryEnabled() {
        return enableDictionary;
    }
}
