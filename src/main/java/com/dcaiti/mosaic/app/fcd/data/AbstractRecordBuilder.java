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

package com.dcaiti.mosaic.app.fcd.data;

import org.eclipse.mosaic.lib.geo.GeoPoint;

/**
 * This abstract builder class is to be used for the building of {@link FxdRecord}-implementations. All common fields are contained
 * within this class. Additional fields can be added by extending this class.
 *
 * @param <BuilderT> concrete type of the builder
 * @param <RecordT>  concrete type of the record
 */
public abstract class AbstractRecordBuilder<BuilderT extends FxdRecord.RecordBuilder<BuilderT, RecordT>, RecordT extends FxdRecord>
        implements FxdRecord.RecordBuilder<BuilderT, RecordT> {

    protected long timeStamp;
    protected GeoPoint position;
    protected String connectionId;
    protected double speed;
    protected double offset;
    protected double heading;

    /**
     * Constructor for the {@link AbstractRecordBuilder} with minimal required parameters.
     */
    public AbstractRecordBuilder(long timeStamp, GeoPoint position, String connectionId) {
        this.timeStamp = timeStamp;
        this.position = position;
        this.connectionId = connectionId;
    }

    /**
     * Constructor for the {@link AbstractRecordBuilder} when copying for a different {@link RecordT Record}.
     *
     * @param record the record to copy from
     */
    public AbstractRecordBuilder(RecordT record) {
        timeStamp = record.timeStamp;
        position = record.position;
        connectionId = record.connectionId;
        speed = record.speed;
        offset = record.offset;
        heading = record.heading;
    }

    @Override
    public BuilderT withOffset(double offset) {
        this.offset = offset;
        return getThis();
    }

    @Override
    public BuilderT withSpeed(double speed) {
        this.speed = speed;
        return getThis();
    }

    @Override
    public BuilderT withHeading(double heading) {
        this.heading = heading;
        return getThis();
    }

    protected abstract BuilderT getThis();

    protected abstract RecordT internalBuild();

    public RecordT build() {
        return internalBuild();
    }
}
