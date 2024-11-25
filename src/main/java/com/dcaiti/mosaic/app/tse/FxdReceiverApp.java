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

package com.dcaiti.mosaic.app.tse;

import com.dcaiti.mosaic.app.fxd.data.FxdRecord;
import com.dcaiti.mosaic.app.fxd.data.FxdTraversal;
import com.dcaiti.mosaic.app.fxd.messages.FxdUpdateMessage;
import com.dcaiti.mosaic.app.tse.config.CFxdReceiverApp;
import com.dcaiti.mosaic.app.tse.processors.FxdProcessor;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.ConfigurableApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.ServerOperatingSystem;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.lib.util.scheduling.EventManager;

/**
 * The {@link FxdReceiverApp} is server that handles message reception and forwards necessary information to the {@link FxdKernel}.
 *
 * @param <RecordT>    Record type (extension of {@link FxdRecord}) containing all spatio-temporal information that units
 *                     periodically transmit to the server
 * @param <TraversalT> Traversal type (extension of {@link FxdTraversal}) containing the traversal of a single connection consisting of a
 *                     list of {@link RecordT Records}.
 * @param <UpdateT>    Updated type (extension of {@link FxdUpdateMessage}) representing the actual messages send by units.
 *                     The most generic implementation contains a collection of records sampled by a unit.
 * @param <ConfigT>    Type of the configuration (extension of {@link CFxdReceiverApp}) in its generic form this class contains
 *                     configuration parameters for the garbage collection and the configured {@link FxdProcessor FxdProcessors}.
 */
public abstract class FxdReceiverApp<
        RecordT extends FxdRecord,
        TraversalT extends FxdTraversal<RecordT, TraversalT>,
        UpdateT extends FxdUpdateMessage<RecordT>,
        ConfigT extends CFxdReceiverApp<RecordT, TraversalT, UpdateT>,
        KernelT extends FxdKernel<RecordT, TraversalT, UpdateT, ConfigT>
        > extends ConfigurableApplication<ConfigT, ServerOperatingSystem> implements CommunicationApplication {

    /**
     * Configuration for the server.
     * See: {@link CFxdReceiverApp}
     */
    protected ConfigT config;
    /**
     * The kernel that handles processing of received applications.
     * See: {@link FxdKernel}
     */
    private KernelT kernel;

    public FxdReceiverApp(Class<? extends ConfigT> configClass) {
        super(configClass);
    }

    @Override
    public void onStartup() {
        getLog().infoSimTime(this, "Starting Server");
        config = getConfiguration();
        enableCellModule();
        kernel = initKernel(getOs().getEventManager(), config);
    }

    @Override
    public void onShutdown() {
        kernel.shutdown(getOs().getSimulationTime());
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage receivedV2xMessage) {
        V2xMessage message = receivedV2xMessage.getMessage();
        if (instanceOfUpdate(message)) {
            getLog().debugSimTime(this, "Received Update from {}", message.getRouting().getSource().getSourceName());
            UpdateT update = castUpdate(message);
            kernel.processUpdate(update);
        } else { // forward message to kernel to potentially be handled by message-based processors
            MessageRouting responseRouting =
                    getOs().getCellModule().createMessageRouting()
                            .destination(message.getRouting().getSource().getSourceName())
                            .topological()
                            .build();
            kernel.handleMessageAdvanced(receivedV2xMessage, responseRouting) // get all responses
                    .forEach(response -> getOs().getCellModule().sendV2xMessage(response)); // send all responses
        }
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
     * This method shall enable the units' cell module with the required configuration.
     */
    public abstract void enableCellModule();

    /**
     * This method shall initialize the extension of the {@link FxdKernel} for the respective implementation.
     *
     * @param eventManager the {@link EventManager} to enable the kernel with event handling capabilities
     * @param config       the extension of the {@link CFxdReceiverApp} containing configuration parameters
     * @return the initialized {@link FxdKernel}
     */
    protected abstract KernelT initKernel(EventManager eventManager, ConfigT config);

    /**
     * This method shall verify that received {@link FxdUpdateMessage} are of the proper type. Typically, it is sufficient to call
     * {@code message instanceof <UpdateT>} in this method.
     *
     * @param message the received {@link V2xMessage}
     * @return {@code true} if the message is an instance of the specialized {@link FxdUpdateMessage}, else {@code false}
     */
    protected abstract boolean instanceOfUpdate(V2xMessage message);

    /**
     * This method shall cast the received {@link V2xMessage} to the proper {@link UpdateT}. Typically, returning the cast message is
     * sufficient.
     *
     * @param message the received {@link V2xMessage}
     * @return a cast version of the received message
     */
    protected abstract UpdateT castUpdate(V2xMessage message);
}
