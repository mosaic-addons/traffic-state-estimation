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

package com.dcaiti.mosaic.app.tse.processors;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.fxd.data.FcdTraversal;
import com.dcaiti.mosaic.app.tse.TseServerApp;
import com.dcaiti.mosaic.app.tse.persistence.FcdDatabaseHelper;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;

import com.google.common.collect.Iterables;

import java.util.LinkedList;

/**
 * Processes new FCD data. Holds methods to compute base-metrics and handles the record-buffer-queue for each veh.
 * It mainly handles computing the spatial and temporal mean speeds, once a veh leaves a connection.
 * This Data needs to be preprocessed, as it is the base from which the thresholds are derived.
 *
 * @see FcdDatabaseHelper
 * @see TseServerApp
 */
public class SpatioTemporalProcessor extends AbstractSpatioTemporalProcessor {

    /**
     * Computes sub-metrics for a traversal and stores them in the database.
     *
     * @param vehicleId vehicle identifier
     * @param traversal {@link FcdTraversal} containing all records of a connection traversal plus previous and following record
     */
    private void computeSubMetrics(String vehicleId, FcdTraversal traversal) {
        ComputedMetrics metrics = computeTraversalMetrics(vehicleId, traversal);
        if (metrics == null) {
            return; // Invalid or incomplete traversal
        }

        String connectionId = traversal.getConnectionId();
        final LinkedList<FcdRecord> records = buildTraversalList(traversal);
        float relativeMetric = -1;

        // insert RTSM as well, if thresholds are already available for the connection
        if (fcdDataStorage.gotThresholdFor(connectionId)) {
            relativeMetric = SpatioTemporalTrafficMetric.computeRelativeTrafficStatusMetric(
                    metrics.temporalMeanSpeed(),
                    metrics.spatialMeanSpeed(),
                    connectionId,
                    fcdDataStorage
            );
        }
        FcdRecord lastRecordOfTraversal = Iterables.getLast(traversal.getTraversal());
        fcdDataStorage.insertTraversalMetrics(
                vehicleId,
                lastRecordOfTraversal.getTimeStamp(),
                connectionId,
                traversal.getFollowingRecord() == null ? connectionId : traversal.getFollowingRecord().getConnectionId(),
                metrics.spatialMeanSpeed(),
                metrics.temporalMeanSpeed(),
                computeNaiveTemporalMeanSpeed(records),
                relativeMetric,
                (long) metrics.traversalTime()
        );
    }

    @Override
    public void initialize(UnitLogger logger) {
        this.logger = logger;
    }

    @Override
    public void shutdown(long shutdownTime) {
        // nop, we could potentially try to calculate metrics for the last connection
    }

    @Override
    public void onConnectionTraversal(String unitId, FcdTraversal traversal) {
        computeSubMetrics(unitId, traversal);
    }
}
