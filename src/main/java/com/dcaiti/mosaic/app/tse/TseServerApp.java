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

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.fxd.data.FcdTraversal;
import com.dcaiti.mosaic.app.fxd.messages.FcdUpdateMessage;
import com.dcaiti.mosaic.app.tse.config.CTseServerApp;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.persistence.FcdDatabaseHelper;
import com.dcaiti.mosaic.app.tse.persistence.ScenarioDatabaseHelper;
import com.dcaiti.mosaic.app.tse.processors.SpatioTemporalProcessor;
import com.dcaiti.mosaic.app.tse.processors.ThresholdProcessor;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.EventManager;

import com.google.common.collect.Lists;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * An extension of {@link FxdReceiverApp} adding the data storage in the form of the {@link FcdDataStorage} and the
 * Network-{@link Database} and configuring default
 * {@link com.dcaiti.mosaic.app.tse.processors.FxdProcessor FxdProcessors}.
 */
public class TseServerApp
        extends FxdReceiverApp<FcdRecord, FcdTraversal, FcdUpdateMessage, CTseServerApp, TseKernel> {

    /**
     * Storage field, allowing to persist TSE results as well as data exchange between processors.
     * Default value will be a {@link FcdDatabaseHelper}
     */
    private FcdDataStorage fcdDataStorage;

    public TseServerApp() {
        super(CTseServerApp.class);
    }

    @Override
    public void enableCellModule() {
        getOs().getCellModule().enable();
    }


    /**
     * Initialization method.
     * Initializes the {@link TseKernel} by
     * <ul>
     *  <li>validating that processors for minimal function are configured,</li>
     *  <li> reading the scenario database for access to network specific data,</li>
     *  <li>and setting up the {@link #fcdDataStorage} to persist TSE metrics.</li>
     * </ul>
     *
     * @param eventManager the {@link EventManager} to enable the kernel with event handling capabilities
     * @param config       configuration for the server
     * @return the initialized {@link TseKernel}
     */
    @Override
    protected TseKernel initKernel(EventManager eventManager, CTseServerApp config) {
        addRequiredProcessors(config);
        Database networkDatabase = ScenarioDatabaseHelper.getNetworkDbFromFile(getOs());
        String databaseDirectory = config.databasePath == null ? getOs().getConfigurationPath().getPath() : getConfiguration().databasePath;
        String databaseFileName = getConfiguration().databaseFileName == null ? "FcdData.sqlite" : getConfiguration().databaseFileName;
        Path databasePath = Paths.get(databaseDirectory, databaseFileName);
        // set data storage to configured type else use default FcdDatabaseHelper
        fcdDataStorage = config.fcdDataStorage == null ? new FcdDatabaseHelper() : config.fcdDataStorage;
        fcdDataStorage.initialize(databasePath, networkDatabase, config.isPersistent, getLog());
        return new TseKernel(eventManager, getLog(), config, fcdDataStorage, networkDatabase);
    }

    private void addRequiredProcessors(CTseServerApp config) {
        if (config.traversalBasedProcessors == null) {
            config.traversalBasedProcessors = Lists.newArrayList(new SpatioTemporalProcessor());
        } else if (config.traversalBasedProcessors.stream().noneMatch(processor -> processor instanceof SpatioTemporalProcessor)) {
            config.traversalBasedProcessors.add(new SpatioTemporalProcessor());
        }
        if (config.timeBasedProcessors == null) {
            config.timeBasedProcessors = Lists.newArrayList(new ThresholdProcessor());
        } else if (config.timeBasedProcessors.stream().noneMatch(processor -> processor instanceof ThresholdProcessor)) {
            config.timeBasedProcessors.add(new ThresholdProcessor());
        }
    }

    @Override
    protected boolean instanceOfUpdate(V2xMessage message) {
        return message instanceof FcdUpdateMessage;
    }

    @Override
    protected FcdUpdateMessage castUpdate(V2xMessage message) {
        return (FcdUpdateMessage) message;
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        getLog().info(fcdDataStorage.getStatisticsString());
        fcdDataStorage.shutdown();
    }
}
