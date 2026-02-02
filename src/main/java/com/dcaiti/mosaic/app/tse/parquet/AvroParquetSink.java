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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

public final class AvroParquetSink<T> implements ParquetSink<T> {
    private final RecordEncoder<T, GenericData.Record> encoder;
    private final GenericData.Record reusableRecord;
    private final ParquetWriter<GenericData.Record> writer;

    AvroParquetSink(Schema schema, ParquetSinkConfig config, RecordEncoder<T, GenericData.Record> encoder) {
        this.encoder = encoder;
        this.reusableRecord = new GenericData.Record(schema);
        this.writer = createWriter(schema, config);
    }

    private ParquetWriter<GenericData.Record> createWriter(Schema schema, ParquetSinkConfig config) {
        try {
            return AvroParquetWriter.<GenericData.Record>builder(OutputFileFactory.create(config.getPath(), config.getFilename()))
                    .withSchema(schema)
                    .withCompressionCodec(config.getCompressionCodec())
                    .withDictionaryEncoding(config.isDictionaryEnabled())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(T value) throws IOException {
        encoder.encode(value, reusableRecord);
        writer.write(reusableRecord);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
