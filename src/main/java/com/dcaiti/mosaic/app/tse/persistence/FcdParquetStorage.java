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

package com.dcaiti.mosaic.app.tse.persistence;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.tse.data.AggregatedMetricRecord;
import com.dcaiti.mosaic.app.tse.data.ConnectionRecord;
import com.dcaiti.mosaic.app.tse.data.IMetricsBuffer;
import com.dcaiti.mosaic.app.tse.data.ThresholdRecord;
import com.dcaiti.mosaic.app.tse.data.TraversalStatistics;
import com.dcaiti.mosaic.app.tse.parquet.FcdGeoParquetSink;
import com.dcaiti.mosaic.app.tse.parquet.ParquetSink;
import com.dcaiti.mosaic.app.tse.parquet.ParquetSinkConfig;
import com.dcaiti.mosaic.app.tse.parquet.ParquetSinks;
import com.dcaiti.mosaic.app.tse.parquet.RecordEncoder;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FcdParquetStorage implements FcdDataStorage {

    private static final Schema TRAVERSAL_METRICS_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "TraversalMetric",
              "fields": [
                {"name": "vehID", "type": "string"},
                {"name": "timeStamp", "type": "long"},
                {"name": "connectionID", "type": "string"},
                {"name": "nextConnectionID", "type": ["null", "string"], "default": null},
                {"name": "temporalMeanSpeed", "type": "double"},
                {"name": "spatialMeanSpeed", "type": "double"},
                {"name": "naiveMeanSpeed", "type": "double"},
                {"name": "relativeTrafficStatusMetric", "type": ["null", "float"], "default": null},
                {"name": "speedPerformanceIndex", "type": ["null", "double"], "default": null},
                {"name": "traversalTime", "type": "long"}
              ]
            }
            """);

    private static final Schema THRESHOLD_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Threshold",
              "fields": [
                {"name": "connectionID", "type": "string"},
                {"name": "temporalThreshold", "type": "double"},
                {"name": "spatialThreshold", "type": "double"},
                {"name": "timeStamp", "type": "long"},
                {"name": "timeOfInsertionUTC", "type": "long"}
              ]
            }
            """);

    private static final Schema CONNECTION_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Connection",
              "fields": [
                {"name": "connectionID", "type": "string"},
                {"name": "maxSpeed", "type": "double"},
                {"name": "length", "type": "double"}
              ]
            }
            """);

    private static final Schema AGGREGATED_METRICS_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "AggregatedMetric",
              "fields": [
                {"name": "connectionID", "type": "string"},
                {"name": "intervalStart", "type": "long"},
                {"name": "intervalEnd", "type": "long"},
                {"name": "avgTemporalMeanSpeed", "type": "double"},
                {"name": "avgSpatialMeanSpeed", "type": "double"},
                {"name": "avgNaiveMeanSpeed", "type": "double"},
                {"name": "avgSpeedPerformanceIndex", "type": "double"},
                {"name": "sampleCount", "type": "int"}
              ]
            }
            """);

    // Parquet sinks for each output file
    private ParquetSink<FcdGeoParquetSink.FcdRecordWithId> fcdSink;
    private ParquetSink<TraversalMetricRecord> traversalSink;
    private ParquetSink<ThresholdRecord> thresholdSink;
    private ParquetSink<ConnectionRecord> connectionSink;
    private ParquetSink<AggregatedMetricRecord> aggregatedSink;

    // Helper record for traversal metrics with all fields
    private record TraversalMetricRecord(
            String vehicleId,
            long timeStamp,
            String connectionId,
            String nextConnectionId,
            double temporalMeanSpeed,
            double spatialMeanSpeed,
            double naiveMeanSpeed,
            Float relativeTrafficStatusMetric,
            Double speedPerformanceIndex,
            long traversalTime
    ) {}

    // In-memory caches for read operations
    private final Map<String, Pair<Double, Double>> thresholdCache = new ConcurrentHashMap<>();
    private final Map<String, List<Long>> traversalTimesCache = new ConcurrentHashMap<>();
    private final ArrayList<TraversalStatistics> traversalMetricsCache = new ArrayList<>();
    private final ArrayList<TraversalMetricRecord> traversalMetricRecordsCache = new ArrayList<>();

    // Statistics counters
    private long fcdRecordCount = 0;
    private long traversalMetricCount = 0;
    private long thresholdCount = 0;
    private long connectionCount = 0;
    private long aggregatedMetricCount = 0;

    private String outputPath;
    private UnitLogger log;

    @Override
    public void initialize(Path databasePath, Database networkDatabase, boolean isPersistent, UnitLogger log) {
        this.log = log;
        this.outputPath = databasePath != null ? databasePath.toString() : null;

        if (outputPath == null) {
            log.info("Initializing Parquet storage at log directory");
        } else {
            log.info("Initializing Parquet storage at: {}", databasePath);
        }

        try {
            // Create FCD records sink (GeoParquet)
            createFcdSink();

            // Create traversal metrics sink
            createTraversalSink();

            // Create thresholds sink
            createThresholdSink();

            // Create aggregated metrics sink
            createAggregatedSink();

            // Create connections sink and write connection data
            createConnectionSink(networkDatabase);

            log.info("Parquet storage initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize Parquet storage", e);
            throw new RuntimeException("Failed to initialize Parquet storage", e);
        }
    }

    private void createFcdSink() {
        ParquetSinkConfig config = ParquetSinkConfig.of(outputPath, "fcd_records.parquet");
        fcdSink = new FcdGeoParquetSink("FcdRecords", config);
        log.info("Created FCD GeoParquet sink");
    }

    private void createTraversalSink() {
        ParquetSinkConfig config = ParquetSinkConfig.of(outputPath, "traversal_metrics.parquet");

        RecordEncoder<TraversalMetricRecord, GenericData.Record> encoder = (metric, record) -> {
            record.put("vehID", metric.vehicleId());
            record.put("timeStamp", metric.timeStamp());
            record.put("connectionID", metric.connectionId());
            record.put("nextConnectionID", metric.nextConnectionId());
            record.put("temporalMeanSpeed", metric.temporalMeanSpeed());
            record.put("spatialMeanSpeed", metric.spatialMeanSpeed());
            record.put("naiveMeanSpeed", metric.naiveMeanSpeed());
            record.put("relativeTrafficStatusMetric", metric.relativeTrafficStatusMetric());
            record.put("speedPerformanceIndex", metric.speedPerformanceIndex());
            record.put("traversalTime", metric.traversalTime());
        };

        traversalSink = ParquetSinks.avro(TRAVERSAL_METRICS_SCHEMA, config, encoder);
        log.info("Created traversal metrics Parquet sink");
    }

    private void createThresholdSink() {
        ParquetSinkConfig config = ParquetSinkConfig.of(outputPath, "thresholds.parquet");

        RecordEncoder<ThresholdRecord, GenericData.Record> encoder = (threshold, record) -> {
            record.put("connectionID", threshold.connectionId());
            record.put("temporalThreshold", threshold.temporalThreshold());
            record.put("spatialThreshold", threshold.spatialThreshold());
            record.put("timeStamp", threshold.timeStamp());
            record.put("timeOfInsertionUTC", threshold.timeOfInsertionUTC());
        };

        thresholdSink = ParquetSinks.avro(THRESHOLD_SCHEMA, config, encoder);
        log.info("Created thresholds Parquet sink");
    }

    private void createAggregatedSink() {
        ParquetSinkConfig config = ParquetSinkConfig.of(outputPath, "aggregated_metrics.parquet");

        RecordEncoder<AggregatedMetricRecord, GenericData.Record> encoder = (aggregated, record) -> {
            record.put("connectionID", aggregated.connectionId());
            record.put("intervalStart", aggregated.intervalStart());
            record.put("intervalEnd", aggregated.intervalEnd());
            record.put("avgTemporalMeanSpeed", aggregated.avgTemporalMeanSpeed());
            record.put("avgSpatialMeanSpeed", aggregated.avgSpatialMeanSpeed());
            record.put("avgNaiveMeanSpeed", aggregated.avgNaiveMeanSpeed());
            record.put("avgSpeedPerformanceIndex", aggregated.avgSpeedPerformanceIndex());
            record.put("sampleCount", aggregated.sampleCount());
        };

        aggregatedSink = ParquetSinks.avro(AGGREGATED_METRICS_SCHEMA, config, encoder);
        log.info("Created aggregated metrics Parquet sink");
    }

    private void createConnectionSink(Database networkDatabase) {
        ParquetSinkConfig config = ParquetSinkConfig.of(outputPath, "connection_data.parquet");

        RecordEncoder<ConnectionRecord, GenericData.Record> encoder = (conn, record) -> {
            record.put("connectionID", conn.connectionId());
            record.put("maxSpeed", conn.maxSpeed());
            record.put("length", conn.length());
        };

        connectionSink = ParquetSinks.avro(CONNECTION_SCHEMA, config, encoder);

        // Write all connection data immediately
        try {
            for (var connection : networkDatabase.getConnections()) {
                ConnectionRecord connRecord = new ConnectionRecord(
                        connection.getId(),
                        connection.getMaxSpeedInMs(),
                        connection.getLength()
                );
                connectionSink.write(connRecord);
                connectionCount++;
            }
            log.info("Created and populated connection data Parquet sink");
        } catch (IOException e) {
            log.error("Failed to write connection data", e);
            throw new RuntimeException("Failed to write connection data", e);
        }
    }

    @Override
    public void insertFcdRecords(Map<String, Collection<FcdRecord>> records) {
        try {
            for (Map.Entry<String, Collection<FcdRecord>> entry : records.entrySet()) {
                String vehicleId = entry.getKey();
                for (FcdRecord record : entry.getValue()) {
                    fcdSink.write(new FcdGeoParquetSink.FcdRecordWithId(vehicleId, record));
                    fcdRecordCount++;
                }
            }
        } catch (IOException e) {
            log.error("Failed to write FCD records to Parquet", e);
            throw new RuntimeException("Failed to write FCD records", e);
        }
    }

    @Override
    public void insertTraversalMetrics(String vehicleId, long timestamp, String connectionId, String nextConnection,
                                       double spatialMeanSpeed, double temporalMeanSpeed, double naiveMeanSpeed,
                                       float relativeMetric, double speedPerformanceIndex, long traversalTime) {
        TraversalMetricRecord metric = new TraversalMetricRecord(
                vehicleId, timestamp, connectionId, nextConnection,
                temporalMeanSpeed, spatialMeanSpeed, naiveMeanSpeed,
                relativeMetric, speedPerformanceIndex, traversalTime
        );

        try {
            traversalSink.write(metric);
            traversalMetricCount++;

            // Cache the full record for potential rewrite
            synchronized (traversalMetricRecordsCache) {
                traversalMetricRecordsCache.add(metric);
            }

            // Update caches for read operations - create TraversalStatistics for cache
            TraversalStatistics stat = new TraversalStatistics(
                    connectionId, -1, timestamp, temporalMeanSpeed, spatialMeanSpeed,
                    relativeMetric, speedPerformanceIndex
            );
            stat.setRelativeTrafficStatusMetric(relativeMetric);

            synchronized (traversalMetricsCache) {
                traversalMetricsCache.add(stat);
            }
            traversalTimesCache
                    .computeIfAbsent(connectionId, k -> new ArrayList<>())
                    .add(traversalTime);

        } catch (IOException e) {
            log.error("Failed to write traversal metric", e);
            throw new RuntimeException("Failed to write traversal metric", e);
        }
    }

    @Override
    public void updateTraversalMetrics(ArrayList<TraversalStatistics> traversals) {
        // For Parquet, we need to rewrite the file with updated RTSM values.
        // We'll close the existing sink and create a new one.
        log.info("Updating traversal metrics in Parquet (rewriting file with {} records)", traversals.size());

        synchronized (traversalMetricsCache) {
            traversalMetricsCache.clear();
            traversalMetricsCache.addAll(traversals);
        }

        // Build a map of (connectionId + timestamp) -> updated RTSM for quick lookup
        Map<String, Float> rtsmUpdates = new HashMap<>();
        for (TraversalStatistics stat : traversals) {
            String key = stat.getConnectionId() + "_" + stat.getTimestamp();
            rtsmUpdates.put(key, stat.getRelativeTrafficStatusMetric());
        }

        // Close existing sink and rewrite file with updated data
        try {
            if (traversalSink != null) {
                traversalSink.close();
            }

            // Delete the existing file before recreating it
            java.nio.file.Path filePath = Paths.get(outputPath, "traversal_metrics.parquet").toAbsolutePath();
            try {
                java.nio.file.Files.deleteIfExists(filePath);
                log.debug("Deleted existing traversal metrics file: {}", filePath);
            } catch (IOException e) {
                log.warn("Could not delete existing traversal metrics file", e);
            }

            // Recreate the sink
            createTraversalSink();

            // Rewrite all traversal metrics with updated RTSM values from the cached records
            traversalMetricCount = 0;
            synchronized (traversalMetricRecordsCache) {
                for (TraversalMetricRecord originalRecord : traversalMetricRecordsCache) {
                    // Look up updated RTSM value by connection ID + timestamp
                    String key = originalRecord.connectionId() + "_" + originalRecord.timeStamp();
                    Float updatedRtsm = rtsmUpdates.getOrDefault(key, originalRecord.relativeTrafficStatusMetric());

                    TraversalMetricRecord updatedRecord = new TraversalMetricRecord(
                            originalRecord.vehicleId(),
                            originalRecord.timeStamp(),
                            originalRecord.connectionId(),
                            originalRecord.nextConnectionId(),
                            originalRecord.temporalMeanSpeed(),
                            originalRecord.spatialMeanSpeed(),
                            originalRecord.naiveMeanSpeed(),
                            updatedRtsm,
                            originalRecord.speedPerformanceIndex(),
                            originalRecord.traversalTime()
                    );

                    traversalSink.write(updatedRecord);
                    traversalMetricCount++;
                }
            }

            log.info("Successfully rewrote {} traversal metrics with updated RTSM values", traversalMetricCount);
        } catch (IOException e) {
            log.error("Failed to update traversal metrics in Parquet", e);
            throw new RuntimeException("Failed to update traversal metrics", e);
        }
    }

    @Override
    public void insertThresholds(Map<String, Double> temporal, Map<String, Double> spatial, long simulationTime) {
        log.debug("insertThresholds called with {} temporal, {} spatial thresholds at {}",
                temporal.size(), spatial.size(), simulationTime);

        if (temporal.isEmpty() && spatial.isEmpty()) {
            log.warn("Both threshold maps are empty!");
            return;
        }

        try {
            long insertionTime = System.currentTimeMillis();
            for (String connectionId : temporal.keySet()) {
                ThresholdRecord threshold = new ThresholdRecord(
                        connectionId,
                        temporal.get(connectionId),
                        spatial.get(connectionId),
                        simulationTime,
                        insertionTime
                );
                thresholdSink.write(threshold);
                thresholdCount++;

                // Update cache
                thresholdCache.put(connectionId,
                        new Pair<>(temporal.get(connectionId), spatial.get(connectionId)));
            }
            log.debug("Wrote {} thresholds to Parquet", thresholdCount);
        } catch (IOException e) {
            log.error("Failed to write thresholds", e);
            throw new RuntimeException("Failed to write thresholds", e);
        }
    }

    @Override
    public void insertSampledMeanSpeeds(IMetricsBuffer metricsBuffer) {
        // This method is currently not implemented in the SQLite version either
        // Can be added later if needed
        log.warn("insertSampledMeanSpeeds is not implemented for Parquet storage");
    }

    @Override
    public void insertAggregatedTraversalMetrics(String connectionId, long intervalStart, long intervalEnd,
                                                 double avgTemporalMeanSpeed, double avgSpatialMeanSpeed,
                                                 double avgNaiveMeanSpeed, double avgSpeedPerformanceIndex,
                                                 int sampleCount) {
        AggregatedMetricRecord record = new AggregatedMetricRecord(
                connectionId, intervalStart, intervalEnd,
                avgTemporalMeanSpeed, avgSpatialMeanSpeed,
                avgNaiveMeanSpeed, avgSpeedPerformanceIndex,
                sampleCount
        );

        try {
            aggregatedSink.write(record);
            aggregatedMetricCount++;
        } catch (IOException e) {
            log.error("Failed to write aggregated metric for connection {} at interval {}-{}",
                    connectionId, intervalStart, intervalEnd, e);
            throw new RuntimeException("Failed to write aggregated metric", e);
        }
    }

    @Override
    public Pair<Double, Double> getThresholds(String connectionId) {
        return thresholdCache.get(connectionId);
    }

    @Override
    public Map<String, List<Long>> getTraversalTimes() {
        return new HashMap<>(traversalTimesCache);
    }

    @Override
    public Map<String, ArrayList<Pair<Double, Double>>> getMeanSpeeds() {
        Map<String, ArrayList<Pair<Double, Double>>> meanSpeeds = new HashMap<>();

        synchronized (traversalMetricsCache) {
            for (TraversalStatistics stat : traversalMetricsCache) {
                meanSpeeds
                        .computeIfAbsent(stat.getConnectionId(), k -> new ArrayList<>())
                        .add(new Pair<>(stat.getTemporalMeanSpeed(), stat.getSpatialMeanSpeed()));
            }
        }

        return meanSpeeds;
    }

    @Override
    public ArrayList<TraversalStatistics> getTraversalMetrics() {
        synchronized (traversalMetricsCache) {
            return new ArrayList<>(traversalMetricsCache);
        }
    }

    @Override
    public TraversalStatistics getClosestTraversalData(String connectionId, long timestamp) {
        TraversalStatistics closest = null;
        long minDiff = Long.MAX_VALUE;

        synchronized (traversalMetricsCache) {
            for (TraversalStatistics stat : traversalMetricsCache) {
                if (stat.getConnectionId().equals(connectionId)) {
                    long diff = Math.abs(stat.getTimestamp() - timestamp);
                    if (diff < minDiff) {
                        minDiff = diff;
                        closest = stat;
                    }
                }
            }
        }

        return closest;
    }

    @Override
    public Map<String, TraversalStatistics> getAveragesForInterval(long timestamp, long interval) {
        Map<String, List<TraversalStatistics>> groupedByConnection = new HashMap<>();
        long startTime = timestamp - interval;

        synchronized (traversalMetricsCache) {
            for (TraversalStatistics stat : traversalMetricsCache) {
                if (stat.getTimestamp() >= startTime && stat.getTimestamp() <= timestamp) {
                    groupedByConnection
                            .computeIfAbsent(stat.getConnectionId(), k -> new ArrayList<>())
                            .add(stat);
                }
            }
        }

        Map<String, TraversalStatistics> averages = new HashMap<>();
        for (Map.Entry<String, List<TraversalStatistics>> entry : groupedByConnection.entrySet()) {
            List<TraversalStatistics> stats = entry.getValue();
            if (!stats.isEmpty()) {
                double avgTemporal = stats.stream().mapToDouble(TraversalStatistics::getTemporalMeanSpeed).average().orElse(0);
                double avgSpatial = stats.stream().mapToDouble(TraversalStatistics::getSpatialMeanSpeed).average().orElse(0);
                double avgNaive = stats.stream().mapToDouble(s -> s.getNaiveMeanSpeed() != null ? s.getNaiveMeanSpeed() : 0).average().orElse(0);
                float avgRtsm = (float) stats.stream()
                        .mapToDouble(s -> s.getRelativeTrafficStatusMetric() != null ? s.getRelativeTrafficStatusMetric() : 0)
                        .average().orElse(0);
                double avgSpi = stats.stream()
                        .mapToDouble(s -> s.getSpeedPerformanceIndex() != null ? s.getSpeedPerformanceIndex() : 0)
                        .average().orElse(0);

                TraversalStatistics avgStat = new TraversalStatistics(
                        entry.getKey(),
                        timestamp,
                        stats.size(),
                        avgTemporal,
                        avgSpatial,
                        avgNaive,
                        avgRtsm,
                        avgSpi
                );
                averages.put(entry.getKey(), avgStat);
            }
        }

        return averages;
    }

    @Override
    public boolean gotThresholdFor(String connectionId) {
        return thresholdCache.containsKey(connectionId);
    }

    @Override
    public void shutdown() {
        log.info("Shutting down Parquet storage...");
        try {
            if (fcdSink != null) {
                fcdSink.close();
            }
            if (traversalSink != null) {
                traversalSink.close();
            }
            if (thresholdSink != null) {
                thresholdSink.close();
            }
            if (connectionSink != null) {
                connectionSink.close();
            }
            if (aggregatedSink != null) {
                aggregatedSink.close();
            }
            log.info("Parquet files written successfully");
            log.info(getStatisticsString());
        } catch (IOException e) {
            log.error("Error closing Parquet writers", e);
        }
    }

    @Override
    public String getStatisticsString() {
        String statisticsString = "Statistics for FCD Database:";
        statisticsString += System.lineSeparator() + "Record Amount: " + fcdRecordCount;
        statisticsString += System.lineSeparator() + "Traversal Amount: " + traversalMetricCount;
        statisticsString += System.lineSeparator() + "Aggregated Metrics Amount: " + aggregatedMetricCount;
        statisticsString += System.lineSeparator() + "Threshold Amount: " + thresholdCount;
        statisticsString += System.lineSeparator() + "Connection Amount: " + connectionCount;
        return statisticsString;
    }
}
