/*
 * Copyright (c) 2021 Fraunhofer FOKUS and others. All rights reserved.
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
import com.dcaiti.mosaic.app.tse.data.DatabaseAccess;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.processors.FxdProcessor;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.scheduling.EventManager;

import java.util.List;

/**
 * An extension of the {@link FxdKernel} for the use case of road vehicles. Additionally, to the root implementation we handle data
 * storage and the network state here in the form of the Network-{@link Database} and a {@link FcdDataStorage}.
 */
public class TseKernel extends FxdKernel<FcdRecord, FcdTraversal, FcdUpdateMessage, CTseServerApp> {

    /**
     * This field is used to hold and store all data relevant for the {@link TseKernel}.
     */
    private final FcdDataStorage fcdDataStorage;

    /**
     * Constructor for the {@link TseKernel}. Compared to the {@link FxdKernel} this includes access to the {@link FcdDataStorage}
     * and the {@link Database} used.
     */
    public TseKernel(EventManager eventManager, UnitLogger logger, CTseServerApp config,
                     FcdDataStorage fcdDataStorage, Database networkDatabase) {
        super(eventManager, logger, config);
        this.fcdDataStorage = fcdDataStorage;
        for (FxdProcessor processor : allProcessors) {
            if (processor instanceof DatabaseAccess databaseAccess) {
                databaseAccess.withDataStorage(networkDatabase, fcdDataStorage);
            }
        }
    }

    @Override
    protected void additionalProcessingOfUpdate(FcdUpdateMessage update) {
    }

    @Override
    protected FcdTraversal createTraversal(List<FcdRecord> traversalRecords, FcdRecord previousRecord, FcdRecord followingRecord) {
        return new FcdTraversal(traversalRecords.get(0).getConnectionId(), traversalRecords, previousRecord, followingRecord);
    }
}
