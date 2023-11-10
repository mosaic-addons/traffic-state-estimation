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

package com.dcaiti.mosaic.app.fxd;

import com.dcaiti.mosaic.app.fxd.config.CFxdTransmitterApp;
import com.dcaiti.mosaic.app.fxd.data.AbstractRecordBuilder;
import com.dcaiti.mosaic.app.fxd.data.FxdRecord;
import com.dcaiti.mosaic.app.fxd.messages.AbstractUpdateMessageBuilder;
import com.dcaiti.mosaic.app.fxd.messages.FxdUpdateMessage;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.ConfigurableApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.geo.GeoUtils;
import org.eclipse.mosaic.lib.objects.road.IConnection;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.lib.util.scheduling.Event;

import org.apache.commons.lang3.StringUtils;

import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class FxdTransmitterApp<
        ConfigT extends CFxdTransmitterApp,
        RecordT extends FxdRecord,
        RecordBuilderT extends AbstractRecordBuilder<RecordBuilderT, RecordT>,
        UpdateT extends FxdUpdateMessage<RecordT>,
        UpdateBuilderT extends AbstractUpdateMessageBuilder<UpdateBuilderT, RecordT, UpdateT>>
        extends ConfigurableApplication<ConfigT, VehicleOperatingSystem> implements VehicleApplication, CommunicationApplication {

    /**
     * Configuration for the sending unit, containing information about package frequency and transmission intervals.
     * See: {@link CFxdTransmitterApp}
     */
    protected ConfigT config;
    /**
     * A time-stamped buffer for {@link RecordT records} that will be sent with the next {@link UpdateT update}.
     */
    protected final SortedMap<Long, RecordT> recordBuffer = new TreeMap<>();

    public FxdTransmitterApp(Class<? extends ConfigT> configClass) {
        super(configClass);
    }

    /**
     * Starts the unit by reading the configuration and enabling the cell module.
     */
    @Override
    public void onStartup() {
        config = getConfiguration();
        enableCellModule();
        getLog().debugSimTime(this, "Startup Transmitter app");
    }

    @Override
    public void onShutdown() {
        if (!recordBuffer.isEmpty()) {
            UpdateT update = prebuildUpdate(getMessageRouting()).isFinal().build(); // set update to be last update
            transmitUpdate(update);
            getLog().debugSimTime(this, "Transmitted Final Update containing {}", update.getRecords().size());
        }
        disableCellModule();
        getLog().debugSimTime(this, "Shutdown Transmitter app");
    }

    @Override
    public void onVehicleUpdated(@Nullable VehicleData previousVehicleData, @Nonnull VehicleData updatedVehicleData) {
        if (updatedVehicleData.getRoadPosition() == null) {
            return;
        }
        long currentTime = updatedVehicleData.getTime();
        handleUpdateTransmission(currentTime);
        handleRecordCollection(updatedVehicleData, currentTime);
    }

    private void handleRecordCollection(@Nonnull VehicleData updatedVehicleData, long currentTime) {
        if (recordBuffer.isEmpty() || currentTime - recordBuffer.lastKey() >= config.collectionInterval) {
            RecordT record = prebuildRecord(updatedVehicleData)
                    .withOffset(calcOffsetBasedOnDistanceDriven(updatedVehicleData))
                    .withSpeed(updatedVehicleData.getSpeed())
                    .withHeading(updatedVehicleData.getHeading())
                    .build();
            if (record != null) {
                recordBuffer.put(currentTime, record);
                getLog().debugSimTime(this, "Collected Record on connection {}", record.getConnectionId());
            }
        }
    }

    private void handleUpdateTransmission(long currentTime) {
        if (!recordBuffer.isEmpty() && currentTime - recordBuffer.firstKey() >= config.transmissionInterval) {
            UpdateT update = prebuildUpdate(getMessageRouting()).build();
            transmitUpdate(update);
            recordBuffer.clear();
            getLog().debugSimTime(this, "Transmitted Update containing {} Records", update.getRecords().size());
        }
    }

    protected MessageRouting getMessageRouting() {
        return getOs().getCellModule().createMessageRouting().topoCast(config.receiverId);
    }

    private void transmitUpdate(UpdateT fxdUpdate) {
        getOs().getCellModule().sendV2xMessage(fxdUpdate);
    }

    /**
     * The connection that was traversed in the previous time step.
     * Note: required for manual offset calculation
     */
    private String previousConnectionId;
    /**
     * Distance to the start node of the current connection.
     * Note: required for manual offset calculation
     */
    private double distanceFromStart;
    /**
     * Counter for the distance driven on the current connection.
     * Note: required for manual offset calculation
     */
    private double distanceDrivenOffset;

    /**
     * As the built-in offset can be error-prone, and a relatively precise offset is needed,
     * this function computes it based on the distance the car has driven since traversing the current connection.
     *
     * @param updatedVehicleData to derive all information from
     * @return double representing the offset in meters
     */
    protected double calcOffsetBasedOnDistanceDriven(VehicleData updatedVehicleData) {
        IConnection currentConnection = updatedVehicleData.getRoadPosition().getConnection();
        if (!StringUtils.equals(currentConnection.getId(), previousConnectionId)) {
            previousConnectionId = currentConnection.getId();

            distanceFromStart = currentConnection.getStartNode().getPosition().distanceTo(updatedVehicleData.getPosition());

            double headingStart = GeoUtils.azimuth(updatedVehicleData.getPosition(), currentConnection.getStartNode().getPosition());
            if (Math.abs(headingStart - updatedVehicleData.getHeading()) < 120) {
                distanceFromStart = -distanceFromStart;
            }

            distanceDrivenOffset = -updatedVehicleData.getDistanceDriven();
        }

        return updatedVehicleData.getDistanceDriven() + distanceDrivenOffset + distanceFromStart;
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage receivedV2xMessage) {

    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement acknowledgement) {

    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {

    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission v2xMessageTransmission) {

    }

    @Override
    public void processEvent(Event event) {

    }

    /**
     * Method, which should enable the units' cellular module with the intended configuration.
     */
    protected abstract void enableCellModule();

    /**
     * Method, which should disable the units' cellular module.
     */
    protected abstract void disableCellModule();

    /**
     * Creates an {@link AbstractRecordBuilder} to be finished with common information (e.g., the offset).
     *
     * @param vehicleData data containing information about the movement
     * @return a prebuild {@link RecordT Record}
     */
    protected abstract RecordBuilderT prebuildRecord(VehicleData vehicleData);

    /**
     * Creates an {@link AbstractUpdateMessageBuilder} containing all information regarding the specialized implementation.
     *
     * @param messageRouting the routing for the update
     * @return an {@link AbstractUpdateMessageBuilder} to finish the creation of the actual {@link UpdateT Update}.
     */
    protected abstract UpdateBuilderT prebuildUpdate(MessageRouting messageRouting);
}
