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

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import org.eclipse.mosaic.lib.geo.GeoPoint;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * A {@link FxdRecord} represents a snapshot of a units current spatio-temporal data, including time, position, and connection id.
 * Based on a collection of these records, different traffic state estimation (TSE) algorithms can be applied.
 * By inheriting from this class, additional information can be added to the record.
 */
public abstract class FxdRecord implements Serializable {

    /**
     * Time at record creation. [ns]
     */
    protected final long timeStamp;
    /**
     * Position at record creation.
     */
    protected final GeoPoint position;
    /**
     * Connection at record creation.
     */
    protected final String connectionId;
    /**
     * Speed at record creation. [m/s]
     */
    protected final double speed;
    /**
     * Distance driven on current connection. [m]
     */
    protected final double offset;
    /**
     * Heading of the unit. [°]
     */
    protected final double heading;

    /**
     * A {@link FxdRecord} represents a snapshot of a units current spatio-temporal data, including time, position, and connection id.
     * Based on a collection of these records, different traffic state estimation (TSE) algorithms can be applied
     */
    protected FxdRecord(long timeStamp, GeoPoint position, String connectionId, double speed, double offset, double heading) {
        this.timeStamp = timeStamp;
        this.position = position;
        this.connectionId = connectionId;
        this.speed = speed;
        this.offset = offset;
        this.heading = heading;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public GeoPoint getPosition() {
        return position;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public double getSpeed() {
        return speed;
    }

    public double getOffset() {
        return offset;
    }

    public double getHeading() {
        return heading;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("timestamp", timeStamp)
                .append("position", position)
                .append("connectionId", connectionId)
                .append("speed", speed)
                .append("offset", offset)
                .append("heading", heading)
                .toString();
    }

    /**
     * Method that estimates the size in Bytes for an {@link FxdRecord}.
     */
    public long calculateRecordSize() {
        return 4L // time stamp
                + 8L * 3L // position (three doubles: lat, lon, ele)
                + 10L // average connection id 10 chars, 1 byte per char
                + 8L // speed
                + 8L // offset
                + 8L; // heading
    }

    /**
     * Interface for the record builder class. Concrete implementations will extend the {@link AbstractRecordBuilder}-class.
     *
     * @param <BuilderT> concrete type of the builder
     * @param <RecordT>  concrete type of the record
     */
    public interface RecordBuilder<BuilderT extends RecordBuilder<BuilderT, RecordT>, RecordT extends FxdRecord> {

        BuilderT withOffset(double offset);

        BuilderT withSpeed(double speed);

        BuilderT withHeading(double heading);
    }
}
