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

public final class ParquetSinks {

    private ParquetSinks() {
    }

    public static <T> ParquetSink<T> avro(Schema schema, ParquetSinkConfig config, RecordEncoder<T, GenericData.Record> encoder) {
        return new AvroParquetSink<>(schema, config, encoder);
    }
}
