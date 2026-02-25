/*
 * Copyright (c) 2026 Fraunhofer FOKUS and others. All rights reserved.
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

import java.io.IOException;
import java.util.Collection;

public interface ParquetSink<T> extends AutoCloseable {

    void write(T value) throws IOException;

    default void writeAll(Collection<? extends T> values) throws IOException {
        for (T value : values) {
            write(value);
        }
    }

    @Override
    void close() throws IOException;
}
