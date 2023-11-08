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

package com.dcaiti.mosaic.app.fcd;

import com.dcaiti.mosaic.app.fcd.config.CFcdTransmitterApp;
import com.dcaiti.mosaic.app.fcd.data.FcdRecord;
import com.dcaiti.mosaic.app.fcd.messages.FcdUpdate;
import org.eclipse.mosaic.fed.application.ambassador.simulation.perception.SimplePerceptionConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.perception.errormodels.WallOcclusion;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;

/**
 * An extension of {@link FxdTransmitterApp}, adding functionality for including perception data.
 */
public class FcdTransmitterApp extends FxdTransmitterApp<CFcdTransmitterApp, FcdRecord, FcdRecord.Builder, FcdUpdate, FcdUpdate.Builder> {

    public FcdTransmitterApp() {
        super(CFcdTransmitterApp.class);
    }

    /**
     * Additionally to calling the functionality of {@link FxdTransmitterApp#onStartup} this method sets up the perception capabilities
     * of the vehicle.
     */
    @Override
    public void onStartup() {
        super.onStartup();
        if (config.includePerceptionData) {
            SimplePerceptionConfiguration.Builder perceptionConfigBuilder
                    = new SimplePerceptionConfiguration.Builder(config.viewingAngle, config.viewingRange);
            if (config.enableWallOcclusion) {
                perceptionConfigBuilder.addModifier(new WallOcclusion());
            }
            getOs().getPerceptionModule().enable(perceptionConfigBuilder.build());
            getLog().debugSimTime(this, "Perception Module enabled.");
        }
    }

    @Override
    protected void enableCellModule() {
        getOs().getCellModule().enable();
    }

    @Override
    protected void disableCellModule() {
        getOs().getCellModule().disable();
    }

    /**
     * This function records the FCD data (vehicleId, time, position, connectionId, offset, speed)
     * with the sample rate of the simulation updates and adds it to the buffer.
     *
     * @param vehicleData contains all data from the update
     */
    @Override
    protected FcdRecord.Builder prebuildRecord(VehicleData vehicleData) {
        FcdRecord.Builder builder =
                new FcdRecord.Builder(vehicleData.getTime(), vehicleData.getPosition(), getOs().getRoadPosition().getConnectionId());
        if (config.includePerceptionData) {
            builder.withPerceivedVehicles(getOs().getPerceptionModule().getPerceivedVehicles());
        }
        return builder;
    }

    @Override
    protected FcdUpdate.Builder prebuildUpdate(MessageRouting messageRouting) {
        return new FcdUpdate.Builder(messageRouting, getOs().getSimulationTime()).addRecords(recordBuffer);
    }

}
