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

package com.dcaiti.mosaic.app.tse.persistence;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.tse.data.IMetricsBuffer;
import com.dcaiti.mosaic.app.tse.data.TraversalStatistics;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;

import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.math3.util.Pair;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonAdapter(FcdDataStorageTypeAdapterFactory.class)
public interface FcdDataStorage {
    /**
     * Inits the DB connection and sets up tables and the cache.
     *
     * @param databasePath    path to fcd database
     * @param networkDatabase the scenario database object
     * @param isPersistent    flag indicating whether tables shall be cleared at startup
     * @param log             to log errors while writing to/reading from {@link FcdDataStorage}
     */
    void initialize(Path databasePath, Database networkDatabase, boolean isPersistent, UnitLogger log);

    /**
     * Does all operations to shut down the DataStorage.
     */
    void shutdown();

    String getStatisticsString();

    void insertFcdRecords(String vehicleId, Collection<FcdRecord> records);

    void insertTraversalMetrics(String vehicleId, long timestamp, String connectionId, String nextConnection,
                                double spatialMeanSpeed, double temporalMeanSpeed, double naiveMeanSpeed,
                                float relativeMetric, long traversalTime);

    void updateTraversalMetrics(ArrayList<TraversalStatistics> traversals);

    void insertThresholds(Map<String, Double> temporal, Map<String, Double> spatial, long simulationTime);

    void insertSampledMeanSpeeds(IMetricsBuffer metricsBuffer);

    Pair<Double, Double> getThresholds(String connectionId);

    Map<String, List<Long>> getTraversalTimes();

    Map<String, ArrayList<Pair<Double, Double>>> getMeanSpeeds();

    ArrayList<TraversalStatistics> getTraversalMetrics();

    TraversalStatistics getClosestTraversalData(String connectionId, long timestamp);

    Map<String, TraversalStatistics> getAveragesForInterval(long timestamp, long interval);

    boolean gotThresholdFor(String connectionId);
}
