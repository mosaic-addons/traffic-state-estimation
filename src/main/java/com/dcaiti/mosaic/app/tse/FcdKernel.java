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

import com.dcaiti.mosaic.app.fcd.config.CTrafficStateEstimationServerApp;
import com.dcaiti.mosaic.app.fcd.data.FcdRecord;
import com.dcaiti.mosaic.app.fcd.data.FcdTraversal;
import com.dcaiti.mosaic.app.fcd.messages.FcdUpdate;
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
public class FcdKernel extends FxdKernel<FcdRecord, FcdTraversal, FcdUpdate, CTrafficStateEstimationServerApp> {

    /**
     * This field is used to hold and store all data relevant for the {@link FcdKernel}.
     */
    private final FcdDataStorage fcdDataStorage;

    /**
     * Constructor for the {@link FcdKernel}. Compared to the {@link FxdKernel} this includes access to the {@link FcdDataStorage}
     * and the {@link Database} used.
     */
    public FcdKernel(EventManager eventManager, UnitLogger logger, CTrafficStateEstimationServerApp config,
                     FcdDataStorage fcdDataStorage, Database networkDatabase) {
        super(eventManager, logger, config);
        this.fcdDataStorage = fcdDataStorage;
        for (FxdProcessor processor : allProcessors) {
            if (processor instanceof DatabaseAccess) {
                DatabaseAccess databaseAccess = (DatabaseAccess) processor;
                databaseAccess.withDataStorage(networkDatabase, fcdDataStorage);
            }
        }
    }

    @Override
    protected void additionalProcessingOfUpdate(FcdUpdate update) {
        if (config.storeRawFcd) {
            fcdDataStorage.insertFcdRecords(update.getRouting().getSource().getSourceName(), update.getRecords().values());
        }
    }

    @Override
    protected FcdTraversal createTraversal(List<FcdRecord> traversalRecords, FcdRecord previousRecord, FcdRecord followingRecord) {
        return new FcdTraversal(traversalRecords.get(0).getConnectionId(), traversalRecords, previousRecord, followingRecord);
    }
}
