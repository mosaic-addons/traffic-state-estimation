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

import org.eclipse.mosaic.fed.application.ambassador.simulation.perception.index.objects.VehicleObject;
import org.eclipse.mosaic.lib.geo.GeoPoint;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class represents a single FCD record containing spatio-temporal information for a single unit.
 */
public class FcdRecord extends FxdRecord {

    /**
     * List of vehicles perceived during the collection of FCD in the form of {@link VehicleObject VehicleObjects}.
     */
    private final List<VehicleObject> perceivedVehicles;

    /**
     * Constructor for a {@link FcdRecord}.
     *
     * @param timeStamp         time of creation
     * @param position          position at creation
     * @param connectionId      connection at creation
     * @param speed             spot speed at creation
     * @param offset            driven distance on current connection
     * @param perceivedVehicles list of surrounding vehicles
     */
    private FcdRecord(long timeStamp, GeoPoint position, String connectionId,
                      double speed, double offset, double heading, List<VehicleObject> perceivedVehicles) {
        super(timeStamp, position, connectionId, speed, offset, heading);
        this.perceivedVehicles = perceivedVehicles;
    }

    public List<VehicleObject> getPerceivedVehicles() {
        return perceivedVehicles;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append(
                        "perceivedVehicles",
                        perceivedVehicles == null
                                ? "disabled perception" : perceivedVehicles.stream().map(VehicleObject::getId).collect(Collectors.toList())
                )
                .toString();
    }

    @Override
    public long calculateRecordSize() {
        return super.calculateRecordSize() + getPerceivedVehicles().size() * 50L; // each perceived vehicle will require around 50 Bytes
    }

    public static class Builder extends AbstractRecordBuilder<Builder, FcdRecord> {

        private List<VehicleObject> perceivedVehicles;

        public Builder(long timeStamp, GeoPoint position, String connectionId) {
            super(timeStamp, position, connectionId);
        }

        public Builder(FcdRecord record) {
            super(record);
            perceivedVehicles = record.perceivedVehicles;
        }

        public Builder withPerceivedVehicles(List<VehicleObject> perceivedVehicles) {
            this.perceivedVehicles = perceivedVehicles;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected FcdRecord internalBuild() {
            return new FcdRecord(timeStamp, position, connectionId, speed, offset, heading, perceivedVehicles);
        }
    }
}
