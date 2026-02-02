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

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import java.io.IOException;
import java.util.Collections;

public final class FcdGeoParquetSink implements ParquetSink<FcdGeoParquetSink.FcdRecordWithId> {

    // Helper class to pair FcdRecord with vehicle ID
    public record FcdRecordWithId(String vehicleId, FcdRecord record) {}

    private static final String FIELD_VEH_ID = "vehID";
    private static final String FIELD_TIMESTAMP = "timeStamp";
    private static final String FIELD_LATITUDE = "latitude";
    private static final String FIELD_LONGITUDE = "longitude";
    private static final String FIELD_GEOMETRY = "geometry";
    private static final String FIELD_CONNECTION_ID = "connectionID";
    private static final String FIELD_OFFSET = "offset";
    private static final String FIELD_SPEED = "speed";
    private static final String FIELD_HEADING = "heading";

    private final ParquetWriter<FcdRecordWithId> parquetWriter;

    public FcdGeoParquetSink(String typeName, ParquetSinkConfig config) {
        MessageType schema = createGeoParquetSchema(typeName);
        Configuration conf = new Configuration();
        OutputFile outputFile = OutputFileFactory.create(config.getPath(), config.getFilename());
        try {
            parquetWriter = new FcdParquetWriterBuilder(outputFile, schema)
                    .withConf(conf)
                    .withCompressionCodec(config.getCompressionCodec())
                    .withDictionaryEncoding(config.isDictionaryEnabled())
                    .withExtraMetaData(Collections.singletonMap("geo", createGeoParquetMetadata()))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String createGeoParquetMetadata() {
        return """
                {
                  "version": "1.0.0",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Point"],
                      "crs": {
                        "type": "name",
                        "properties": {
                          "name": "EPSG:4326"
                        }
                      }
                    }
                  }
                }
                """;
    }

    private static MessageType createGeoParquetSchema(String typeName) {
        return Types.buildMessage()
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(FIELD_VEH_ID))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                        .named(FIELD_TIMESTAMP))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(FIELD_LATITUDE))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(FIELD_LONGITUDE))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .named(FIELD_GEOMETRY))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(FIELD_CONNECTION_ID))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(FIELD_OFFSET))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(FIELD_SPEED))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named(FIELD_HEADING))
                .named(typeName);
    }

    @Override
    public void write(FcdRecordWithId record) throws IOException {
        parquetWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }

    private static final class FcdParquetWriterBuilder extends ParquetWriter.Builder<FcdRecordWithId, FcdParquetWriterBuilder> {
        private final MessageType schema;

        private FcdParquetWriterBuilder(OutputFile outputFile, MessageType schema) {
            super(outputFile);
            this.schema = schema;
        }

        @Override
        protected FcdParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<FcdRecordWithId> getWriteSupport(Configuration conf) {
            return new FcdRecordWriteSupport(schema);
        }
    }

    private static final class FcdRecordWriteSupport extends WriteSupport<FcdRecordWithId> {
        private final MessageType schema;
        private final GeometryFactory geometryFactory = new GeometryFactory();
        private final WKBWriter wkbWriter = new WKBWriter();
        private RecordConsumer recordConsumer;

        private FcdRecordWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, Collections.emptyMap());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(FcdRecordWithId recordWithId) {
            recordConsumer.startMessage();

            // vehID (required)
            int fieldIndex = schema.getFieldIndex(FIELD_VEH_ID);
            recordConsumer.startField(FIELD_VEH_ID, fieldIndex);
            recordConsumer.addBinary(Binary.fromString(recordWithId.vehicleId));
            recordConsumer.endField(FIELD_VEH_ID, fieldIndex);

            // timeStamp (required)
            fieldIndex = schema.getFieldIndex(FIELD_TIMESTAMP);
            recordConsumer.startField(FIELD_TIMESTAMP, fieldIndex);
            recordConsumer.addLong(recordWithId.record.getTimeStamp());
            recordConsumer.endField(FIELD_TIMESTAMP, fieldIndex);

            // latitude (required)
            fieldIndex = schema.getFieldIndex(FIELD_LATITUDE);
            recordConsumer.startField(FIELD_LATITUDE, fieldIndex);
            recordConsumer.addDouble(recordWithId.record.getPosition().getLatitude());
            recordConsumer.endField(FIELD_LATITUDE, fieldIndex);

            // longitude (required)
            fieldIndex = schema.getFieldIndex(FIELD_LONGITUDE);
            recordConsumer.startField(FIELD_LONGITUDE, fieldIndex);
            recordConsumer.addDouble(recordWithId.record.getPosition().getLongitude());
            recordConsumer.endField(FIELD_LONGITUDE, fieldIndex);

            // geometry (optional WKB Point)
            Point point = geometryFactory.createPoint(
                    new Coordinate(
                            recordWithId.record.getPosition().getLongitude(),
                            recordWithId.record.getPosition().getLatitude()
                    )
            );
            byte[] wkb = wkbWriter.write(point);
            fieldIndex = schema.getFieldIndex(FIELD_GEOMETRY);
            recordConsumer.startField(FIELD_GEOMETRY, fieldIndex);
            recordConsumer.addBinary(Binary.fromConstantByteArray(wkb));
            recordConsumer.endField(FIELD_GEOMETRY, fieldIndex);

            // connectionID (required)
            fieldIndex = schema.getFieldIndex(FIELD_CONNECTION_ID);
            recordConsumer.startField(FIELD_CONNECTION_ID, fieldIndex);
            recordConsumer.addBinary(Binary.fromString(recordWithId.record.getConnectionId()));
            recordConsumer.endField(FIELD_CONNECTION_ID, fieldIndex);

            // offset (required)
            fieldIndex = schema.getFieldIndex(FIELD_OFFSET);
            recordConsumer.startField(FIELD_OFFSET, fieldIndex);
            recordConsumer.addDouble(recordWithId.record.getOffset());
            recordConsumer.endField(FIELD_OFFSET, fieldIndex);

            // speed (required)
            fieldIndex = schema.getFieldIndex(FIELD_SPEED);
            recordConsumer.startField(FIELD_SPEED, fieldIndex);
            recordConsumer.addDouble(recordWithId.record.getSpeed());
            recordConsumer.endField(FIELD_SPEED, fieldIndex);

            // heading (required)
            fieldIndex = schema.getFieldIndex(FIELD_HEADING);
            recordConsumer.startField(FIELD_HEADING, fieldIndex);
            recordConsumer.addDouble(recordWithId.record.getHeading());
            recordConsumer.endField(FIELD_HEADING, fieldIndex);

            recordConsumer.endMessage();
        }
    }
}
