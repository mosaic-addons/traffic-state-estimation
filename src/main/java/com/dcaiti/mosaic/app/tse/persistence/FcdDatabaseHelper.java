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
import com.dcaiti.mosaic.app.tse.config.CTseServerApp;
import com.dcaiti.mosaic.app.tse.data.IMetricsBuffer;
import com.dcaiti.mosaic.app.tse.data.TraversalStatistics;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;

import jdk.jfr.Experimental;
import org.apache.commons.math3.util.Pair;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to connect to FcdDatabase and perform queries.
 */
public class FcdDatabaseHelper implements FcdDataStorage {
    // DB strings
    private static final String TABLE_RECORDS = "fcd_records";
    private static final String TABLE_TRAVERSAL_METRICS = "traversal_metrics";
    private static final String TABLE_THRESHOLDS = "thresholds_for_connections";
    private static final String TABLE_CONNECTIONS = "connection_data";
    private static final String COLUMN_NEXT_CONNECTION_ID = "nextConnectionID";
    protected static final String COLUMN_CONNECTION_ID = "connectionID";
    protected static final String COLUMN_TIME_STAMP = "timeStamp";
    private static final String COLUMN_TEMPORAL_MEAN_SPEED = "temporalMeanSpeed";
    private static final String COLUMN_SPATIAL_MEAN_SPEED = "spatialMeanSpeed";
    private static final String COLUMN_TIME_OF_INSERTION = "timeOfInsertionUTC";
    private static final String COLUMN_VEH_ID = "vehID";
    private static final String COLUMN_TRAVERSAL_TIME = "traversalTime";
    private static final String COLUMN_NAIVE_MEAN_SPEED = "naiveMeanSpeed";
    private static final String COLUMN_MAX_ALLOWED_SPEED = "maxSpeed";
    private static final String COLUMN_LENGTH_BY_NODES = "length";
    private static final String COLUMN_OFFSET = "offset";
    private static final String COLUMN_LATITUDE = "latitude";
    private static final String COLUMN_LONGITUDE = "longitude";
    private static final String COLUMN_SPEED = "speed";
    private static final String COLUMN_TEMPORAL_THRESHOLD = "temporalThreshold";
    private static final String COLUMN_SPATIAL_THRESHOLD = "spatialThreshold";
    private static final String COLUMN_RELATIVE_TRAFFIC_METRIC = "relativeTrafficStatusMetric";
    /**
     * Configurable parameter using the {@link CTseServerApp#fcdDataStorage}
     * type-based config. If this is set to {@code true} all sqlite transactions will be handled in-memory, which may lead to increased
     * RAM usage but could also gain performance improvements.
     * Note: if simulation crashes during execution, all data will be lost as the backup is executed in the {@link #shutdown()} method.
     */
    @Experimental
    public boolean inMemory = false;
    /**
     * The {@link Database Network Database}.
     */
    private Database networkDb;
    /**
     * Path of the FCD database.
     */
    private Path databasePath;
    /**
     * Caches the thresholds to not always load them from the database.
     */
    private final Map<String, Pair<Double, Double>> thresholdCache = new HashMap<>();
    /**
     * Logger for errors during writing to/reading from databases.
     */
    private UnitLogger logger;
    /**
     * needed to determine if a record is from this simulation, or a prior one, if running in persistent mode.
     */
    private long startTime;
    /**
     * Field for the {@link Connection Database connection} we keep this connection alive throughout the simulation to reduce overhead
     * through connect/close calls.
     */
    protected Connection connection;

    /**
     * Inits the DB connection and sets up tables and the cache.
     *
     * @param databasePath    path to fcd database
     * @param networkDatabase the scenario database object
     * @param isPersistent    flag indicating whether tables shall be cleared at startup
     * @param logger          to log errors while writing to/reading from {@link FcdDataStorage}
     */
    @Override
    public void initialize(Path databasePath, Database networkDatabase, boolean isPersistent, UnitLogger logger) {
        this.databasePath = databasePath;
        this.startTime = System.currentTimeMillis() / 1000L;
        this.networkDb = networkDatabase;
        this.logger = logger;
        connection = getConnection();
        try {
            createTables(isPersistent);
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "TABLE CREATION");
            throw new RuntimeException("Couldn't create database tables:", exception);
        }
        initializeCache();
        copyConnectionsData(networkDatabase);

    }

    /**
     * Establish connection to DB.
     *
     * @return Connection to SQLiteDB
     */
    private Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                String url = "jdbc:sqlite:";
                if (!inMemory) {
                    url += databasePath;
                }
                connection = DriverManager.getConnection(url);
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "CONNECT");
        }
        return connection;
    }

    @Override
    public void shutdown() {
        String backupSql = "BACKUP TO " + databasePath.toAbsolutePath().normalize();
        if (inMemory) {
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.executeUpdate(backupSql);
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException exception) {
                logErrorAndThrowRuntimeException(exception, "WRITING IN-MEMORY DB");
            }
        }
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "CLOSE");
        }
    }

    /**
     * Creates database tables. Overwrites traversal {@link #TABLE_RECORDS}, {@link #TABLE_CONNECTIONS}, {@link #TABLE_TRAVERSAL_METRICS},
     * and {@link #TABLE_THRESHOLDS} if the database is set to be non-persistent.
     *
     * @param isPersistent flag indicating whether tables shall be cleared at startup
     */
    protected void createTables(boolean isPersistent) throws SQLException {
        String recordsSql =
                "CREATE TABLE IF NOT EXISTS " + TABLE_RECORDS + " ("
                        + COLUMN_VEH_ID + " TEXT NOT NULL, "
                        + COLUMN_TIME_STAMP + " INTEGER NOT NULL, "
                        + COLUMN_LATITUDE + " REAL NO NULL, "
                        + COLUMN_LONGITUDE + " REAL NO NULL, "
                        + COLUMN_CONNECTION_ID + " TEXT NOT NULL, "
                        + COLUMN_OFFSET + " INTEGER NOT NULL, "
                        + COLUMN_SPEED + " INTEGER NOT NULL, "
                        + "PRIMARY KEY (" + COLUMN_CONNECTION_ID + ", " + COLUMN_TIME_STAMP + ", " + COLUMN_VEH_ID + ")"
                        + "); "
                        + "CREATE INDEX timeStamp_index ON " + TABLE_RECORDS + " (" + COLUMN_TIME_STAMP + "); "
                        + "CREATE INDEX connection_index ON " + TABLE_RECORDS + " (" + COLUMN_CONNECTION_ID + "); ";
        String traversalMetricsSql =
                "CREATE TABLE IF NOT EXISTS " + TABLE_TRAVERSAL_METRICS + " ("
                        + COLUMN_VEH_ID + " TEXT NOT NULL, "
                        + COLUMN_TIME_STAMP + " INTEGER NOT NULL, "
                        + COLUMN_CONNECTION_ID + " TEXT NOT NULL, "
                        + COLUMN_NEXT_CONNECTION_ID + " TEXT, "
                        + COLUMN_TEMPORAL_MEAN_SPEED + " INTEGER NOT NULL, "
                        + COLUMN_SPATIAL_MEAN_SPEED + " INTEGER NOT NULL, "
                        + COLUMN_RELATIVE_TRAFFIC_METRIC + " INTEGER, "
                        + COLUMN_TRAVERSAL_TIME + " INTEGER NOT NULL,"
                        + COLUMN_NAIVE_MEAN_SPEED + " INTEGER NOT NULL,"
                        + COLUMN_TIME_OF_INSERTION + " DATETIME DEFAULT CURRENT_TIMESTAMP"
                        + "); "
                        + "CREATE INDEX timeStamp_index ON " + TABLE_TRAVERSAL_METRICS + " (" + COLUMN_TIME_STAMP + "); "
                        + "CREATE INDEX connection_index ON " + TABLE_TRAVERSAL_METRICS + " (" + COLUMN_CONNECTION_ID + "); "
                        + "CREATE INDEX insertion_index ON " + TABLE_TRAVERSAL_METRICS + " (" + COLUMN_TIME_OF_INSERTION + "); ";
        String thresholdsSql =
                "CREATE TABLE IF NOT EXISTS " + TABLE_THRESHOLDS + " ("
                        + COLUMN_CONNECTION_ID + " TEXT NOT NULL, "
                        + COLUMN_TEMPORAL_THRESHOLD + " INTEGER NOT NULL, "
                        + COLUMN_SPATIAL_THRESHOLD + " INTEGER NOT NULL,"
                        + COLUMN_TIME_STAMP + " INTEGER NOT NULL,"
                        + COLUMN_TIME_OF_INSERTION + " DATETIME DEFAULT CURRENT_TIMESTAMP"
                        + ");"
                        + "CREATE INDEX connection_index ON " + TABLE_THRESHOLDS + " (" + COLUMN_CONNECTION_ID + "); "
                        + "CREATE INDEX insertion_index ON " + TABLE_THRESHOLDS + " (" + COLUMN_TIME_OF_INSERTION + "); ";
        String connectionsTableSql =
                "CREATE TABLE IF NOT EXISTS " + TABLE_CONNECTIONS + " ("
                        + COLUMN_CONNECTION_ID + " TEXT NOT NULL, "
                        + COLUMN_MAX_ALLOWED_SPEED + " INTEGER NOT NULL, "
                        + COLUMN_LENGTH_BY_NODES + " INTEGER NOT NULL"
                        + "); "
                        + "DELETE FROM " + TABLE_CONNECTIONS + "; "
                        + "CREATE INDEX connection_index ON " + TABLE_CONNECTIONS + " (" + COLUMN_CONNECTION_ID + "); ";
        if (inMemory) { // read from existing backup if a database exists
            String restoreDatabaseSql = "RESTORE FROM " + databasePath;
            if (Files.exists(databasePath.toAbsolutePath().normalize())) {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(restoreDatabaseSql);
                }
            }
        }
        if (!isPersistent) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + TABLE_RECORDS + " ;");
                statement.execute("DROP TABLE IF EXISTS " + TABLE_CONNECTIONS + ";");
                statement.execute("DROP TABLE IF EXISTS " + TABLE_TRAVERSAL_METRICS + ";");
                statement.execute("DROP TABLE IF EXISTS " + TABLE_THRESHOLDS + ";");
            }
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(recordsSql);
            statement.execute(traversalMetricsSql);
            statement.execute(thresholdsSql);
            statement.execute(connectionsTableSql);
        }
    }

    /**
     * This method allows to write records of many units with one sql batch statement.
     *
     * @param records a map containing the vehicleId is key and it's corresponding records as value
     */
    @Override
    public void insertFcdRecords(Map<String, Collection<FcdRecord>> records) {
        String sqlRecordInsert = "REPLACE INTO " + TABLE_RECORDS + "("
                + COLUMN_VEH_ID + ","
                + COLUMN_TIME_STAMP + ","
                + COLUMN_LATITUDE + ","
                + COLUMN_LONGITUDE + ","
                + COLUMN_CONNECTION_ID + ","
                + COLUMN_OFFSET + ","
                + COLUMN_SPEED + ")"
                + " VALUES(?,?,?,?,?,?,?)";

        try (PreparedStatement statement = connection.prepareStatement(sqlRecordInsert)) {
            connection.setAutoCommit(false);
            int i = 0;
            for (String vehicleId : records.keySet()) {
                for (FcdRecord record : records.get(vehicleId)) {
                    statement.setString(1, vehicleId);
                    statement.setLong(2, record.getTimeStamp());
                    statement.setDouble(3, record.getPosition().getLatitude());
                    statement.setDouble(4, record.getPosition().getLongitude());
                    statement.setString(5, record.getConnectionId());
                    statement.setDouble(6, record.getOffset());
                    statement.setDouble(7, record.getSpeed());
                    statement.addBatch();
                    i++;
                    if (i % 1000 == 0 || i == records.size()) {
                        statement.executeBatch();
                        connection.commit();
                    }
                }
            }
            statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "RECORD INSERTION");
        }
    }

    /**
     * Insert the given metrics as a traversal in the database.
     *
     * @param vehicleId         id of the Veh
     * @param timestamp         timestamp of last record of this traversal
     * @param connectionId      id of the connection
     * @param nextConnection    id of the connection onto which the veh left this one
     * @param spatialMeanSpeed  mean speed over equidistant points on the connection
     * @param temporalMeanSpeed mean speed over the hole traversal as length/traversal time
     * @param naiveMeanSpeed    averaged speeds of the records
     * @param relativeMetric    relative traffic metric as proposed by Yoon et. al.
     * @param traversalTime     time it took to traverse this connection
     */
    @Override
    public void insertTraversalMetrics(String vehicleId, long timestamp, String connectionId,
                                       String nextConnection, double spatialMeanSpeed, double temporalMeanSpeed,
                                       double naiveMeanSpeed, float relativeMetric, long traversalTime) {
        String sqlSubMetricInsert = "INSERT INTO " + TABLE_TRAVERSAL_METRICS + "("
                + COLUMN_VEH_ID + ","
                + COLUMN_TIME_STAMP + ","
                + COLUMN_CONNECTION_ID + ","
                + COLUMN_NEXT_CONNECTION_ID + ","
                + COLUMN_SPATIAL_MEAN_SPEED + ","
                + COLUMN_TEMPORAL_MEAN_SPEED + ","
                + COLUMN_RELATIVE_TRAFFIC_METRIC + ","
                + COLUMN_TRAVERSAL_TIME + ", "
                + COLUMN_NAIVE_MEAN_SPEED + ")"
                + " VALUES(?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sqlSubMetricInsert)) {
            statement.setString(1, vehicleId);
            statement.setLong(2, timestamp);
            statement.setString(3, connectionId);
            statement.setString(4, nextConnection);
            statement.setDouble(5, spatialMeanSpeed);
            statement.setDouble(6, temporalMeanSpeed);
            statement.setFloat(7, relativeMetric);
            statement.setLong(8, traversalTime);
            statement.setDouble(9, naiveMeanSpeed);
            statement.execute();
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "TRAVERSAL METRIC INSERTION");
        }
    }

    /**
     * Updates traversal metrics with relative traffic metric.
     *
     * @param traversals a list of FcdTraversals with RTSMs to update them in the DB
     */
    @Override
    public void updateTraversalMetrics(ArrayList<TraversalStatistics> traversals) {
        String thresholdInsertSql = "UPDATE " + TABLE_TRAVERSAL_METRICS + " SET "
                + COLUMN_RELATIVE_TRAFFIC_METRIC + " = ?"
                + " WHERE ROWID = ?";
        try (PreparedStatement statement = connection.prepareStatement(thresholdInsertSql)) {
            connection.setAutoCommit(false);
            int i = 0;
            for (TraversalStatistics traversal : traversals) {
                if (traversal.getRelativeTrafficStatusMetric() == null) {
                    continue;
                }
                statement.setFloat(1, traversal.getRelativeTrafficStatusMetric());
                statement.setInt(2, traversal.getTraversalId());
                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == traversals.size()) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "TRAVERSAL METRIC UPDATE");
        }
    }

    /**
     * inserts thresholds into THRESHOLDS_TABLE in DB and update {@code thresholdsCache}.
     *
     * @param temporalThresholds mapping of connectionID to a temporal threshold
     * @param spatialThresholds  mapping of connectionID to a spatial threshold
     * @param simulationTime     Time at which the threshold computation started. Needed for further analysis of the resulting data.
     */
    @Override
    public void insertThresholds(Map<String, Double> temporalThresholds, Map<String, Double> spatialThresholds, long simulationTime) {
        String thresholdInsertSql = "INSERT INTO " + TABLE_THRESHOLDS + "("
                + COLUMN_CONNECTION_ID + ","
                + COLUMN_TEMPORAL_THRESHOLD + ","
                + COLUMN_SPATIAL_THRESHOLD + ","
                + COLUMN_TIME_STAMP + ")"
                + " VALUES(?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(thresholdInsertSql)) {
            connection.setAutoCommit(false);
            int i = 0;
            for (Map.Entry<String, Double> temporalThreshold : temporalThresholds.entrySet()) {
                statement.setString(1, temporalThreshold.getKey());
                statement.setDouble(2, temporalThreshold.getValue());
                statement.setDouble(3, spatialThresholds.get(temporalThreshold.getKey()));
                statement.setLong(4, simulationTime);
                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == temporalThresholds.entrySet().size()) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "THRESHOLD INSERTION");
        }
        // cache thresholds in the threshold cache
        temporalThresholds.forEach((conn, tempT) -> thresholdCache.put(conn, new Pair<>(tempT, spatialThresholds.get(conn))));
    }

    @Override
    public void insertSampledMeanSpeeds(IMetricsBuffer metricsBuffer) {
        // part of closed source
    }

    /**
     * Used to copy information about the connections from mosaic DB to the FcdDatabase.
     * This Data is needed for evaluation of the results.
     *
     * @param mosaicDatabase The DB containing data about the road network.
     */
    private void copyConnectionsData(Database mosaicDatabase) {
        String conDataSql = "INSERT OR IGNORE INTO " + TABLE_CONNECTIONS + "("
                + COLUMN_CONNECTION_ID + ","
                + COLUMN_MAX_ALLOWED_SPEED + ","
                + COLUMN_LENGTH_BY_NODES + ")"
                + " VALUES(?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(conDataSql)) {
            connection.setAutoCommit(false);
            Collection<org.eclipse.mosaic.lib.database.road.Connection> connections = mosaicDatabase.getConnections();
            int i = 0;
            for (org.eclipse.mosaic.lib.database.road.Connection roadConnection : connections) {
                statement.setString(1, roadConnection.getId());
                statement.setDouble(2, roadConnection.getMaxSpeedInMs());
                statement.setDouble(3, ScenarioDatabaseHelper.calcLengthByNodes(roadConnection));
                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == connections.size()) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "SCENARIO DB COPYING");
        }
    }

    /**
     * Returns most recent thresholds for connection from cache or DB, if not in cache.
     *
     * @param connectionId for which thresholds shall be returned
     * @return a {@link Pair} of the temporal and spatial threshold
     */
    public Pair<Double, Double> getThresholds(String connectionId) {
        if (thresholdCache.containsKey(connectionId)) {
            return thresholdCache.get(connectionId);
        }
        String thresholdSql = "SELECT "
                + COLUMN_TEMPORAL_THRESHOLD + ", " + COLUMN_SPATIAL_THRESHOLD
                + " FROM " + TABLE_THRESHOLDS
                + " WHERE " + COLUMN_CONNECTION_ID + " = ? "
                + " ORDER BY " + COLUMN_TIME_OF_INSERTION + " DESC "
                + "LIMIT 1;";

        try (PreparedStatement statement = connection.prepareStatement(thresholdSql)) {
            statement.setString(1, connectionId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Pair<Double, Double> thresholdPair =
                        new Pair<>(rs.getDouble(COLUMN_TEMPORAL_THRESHOLD), rs.getDouble(COLUMN_SPATIAL_THRESHOLD));
                thresholdCache.put(connectionId, thresholdPair);
                return thresholdPair;
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "THRESHOLD RETRIEVAL");
            return null;
        }
    }

    /**
     * retrieves all traversal times inserted since the start of this simulation as a mapping of connectionID to a list of traversalTimes.
     *
     * @return mapping of connectionID to List of traversalTimes
     */
    public Map<String, List<Long>> getTraversalTimes() {
        String traversalsSql = "SELECT "
                + COLUMN_CONNECTION_ID + ", " + COLUMN_TRAVERSAL_TIME
                + " FROM " + TABLE_TRAVERSAL_METRICS
                + " ORDER BY " + COLUMN_CONNECTION_ID
                + ";";
        try (PreparedStatement statement = connection.prepareStatement(traversalsSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                Map<String, List<Long>> timeResults = new HashMap<>();
                while (rs.next()) {
                    timeResults.putIfAbsent(rs.getString(COLUMN_CONNECTION_ID), new ArrayList<>());
                    timeResults.get(rs.getString(COLUMN_CONNECTION_ID)).add(rs.getLong(COLUMN_TRAVERSAL_TIME));
                }
                return timeResults;
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "TRAVERSAL TIME RETRIEVAL");
            return null;
        }
    }


    /**
     * Returns all spatio-temporal mean speeds for each connection from the DB.
     *
     * @return map of connection ids to a list of {@link Pair pairs} of temporal and spatial mean speeds
     */
    @Override
    public Map<String, ArrayList<Pair<Double, Double>>> getMeanSpeeds() {
        String percentRanksSql = "SELECT "
                + COLUMN_CONNECTION_ID + ", " + COLUMN_TEMPORAL_MEAN_SPEED + ", " + COLUMN_SPATIAL_MEAN_SPEED
                + " FROM " + TABLE_TRAVERSAL_METRICS
                + " ORDER BY " + COLUMN_CONNECTION_ID
                + ";";
        try (PreparedStatement statement = connection.prepareStatement(percentRanksSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                Map<String, ArrayList<Pair<Double, Double>>> tempThreshResults = new HashMap<>();
                while (rs.next()) {
                    tempThreshResults.putIfAbsent(rs.getString(COLUMN_CONNECTION_ID), new ArrayList<>());
                    tempThreshResults.get(rs.getString(COLUMN_CONNECTION_ID))
                            .add(new Pair<>(rs.getDouble(COLUMN_TEMPORAL_MEAN_SPEED), rs.getDouble(COLUMN_SPATIAL_MEAN_SPEED)));
                }
                return tempThreshResults;
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "MEAN SPEED RETRIEVAL");
            return null;
        }
    }

    /**
     * Retrieves all traversal metrics from the database that were added during this simulation.
     *
     * @return an ArrayList of FcdTraversals
     */
    @Override
    public ArrayList<TraversalStatistics> getTraversalMetrics() {
        String traversalMetricsSql = "SELECT "
                + COLUMN_CONNECTION_ID + ", ROWID AS travID , " + COLUMN_TIME_STAMP
                + ", " + COLUMN_TEMPORAL_MEAN_SPEED + ", " + COLUMN_SPATIAL_MEAN_SPEED
                + " FROM " + TABLE_TRAVERSAL_METRICS
                + " WHERE " + COLUMN_TIME_OF_INSERTION + " >= DATETIME(" + startTime + ", 'unixepoch') "
                + " ORDER BY " + COLUMN_CONNECTION_ID
                + ";";
        try (PreparedStatement statement = connection.prepareStatement(traversalMetricsSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                ArrayList<TraversalStatistics> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(
                            new TraversalStatistics(
                                    rs.getString(COLUMN_CONNECTION_ID), rs.getInt("travID"),
                                    rs.getLong(COLUMN_TIME_STAMP), rs.getDouble(COLUMN_TEMPORAL_MEAN_SPEED),
                                    rs.getDouble(COLUMN_SPATIAL_MEAN_SPEED)
                            )
                    );
                }
                return results;
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "TRAVERSAL METRIC RETRIEVAL");
            return null;
        }
    }

    /**
     * Retrieves the closest traversal data for a given connection and time.
     *
     * @param connectionId of connection
     * @param timestamp    to get the closest data for
     * @return an FcdTraversal with the retrieved data
     */
    @Override
    public TraversalStatistics getClosestTraversalData(String connectionId, long timestamp) {
        String traversalDataSql = "WITH connTravs AS ("
                + "SELECT ROWID AS travID, * FROM " + TABLE_TRAVERSAL_METRICS
                + " WHERE " + COLUMN_CONNECTION_ID + " = '?' "
                + ")"
                + "SELECT " + COLUMN_CONNECTION_ID + ", travID, " + COLUMN_TIME_STAMP + ", " + COLUMN_TEMPORAL_MEAN_SPEED
                + ", " + COLUMN_SPATIAL_MEAN_SPEED + ", " + COLUMN_RELATIVE_TRAFFIC_METRIC
                + " FROM ("
                + "SELECT max(" + COLUMN_TIME_STAMP + ") AS ts, * FROM connTravs WHERE " + COLUMN_TIME_STAMP + " <= ?"
                + " UNION "
                + "SELECT min(" + COLUMN_TIME_STAMP + ") AS ts, * FROM connTravs WHERE " + COLUMN_TIME_STAMP + " <= ?"
                + ")"
                + "WHERE " + COLUMN_TIME_STAMP + " is not null "
                + "ORDER BY abs(?-ts) LIMIT 1;";
        try (PreparedStatement statement = connection.prepareStatement(traversalDataSql)) {
            statement.setString(1, connectionId);
            statement.setLong(2, timestamp);
            statement.setLong(3, timestamp);
            statement.setLong(4, timestamp);
            try (ResultSet rs = statement.executeQuery(traversalDataSql)) {
                if (rs.next()) {
                    return new TraversalStatistics(
                            rs.getString(COLUMN_CONNECTION_ID), rs.getInt("travID"),
                            rs.getLong(COLUMN_TIME_STAMP), rs.getDouble(COLUMN_TEMPORAL_MEAN_SPEED),
                            rs.getDouble(COLUMN_SPATIAL_MEAN_SPEED), rs.getFloat(COLUMN_RELATIVE_TRAFFIC_METRIC),
                            getSpeedPerformanceIndexFor(rs.getDouble(COLUMN_TEMPORAL_MEAN_SPEED), rs.getString(COLUMN_CONNECTION_ID))
                    );
                } else {
                    return null;
                }
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "CONNECTION TRAVERSAL DATA RETRIEVAL");
            return null;
        }
    }

    /**
     * Gets a map of connection ids to FcdTraversals containing the AVGs over the given interval.
     *
     * @param timestamp start of the interval
     * @param interval  interval length
     * @return mapping of connection IDs to FcdTraversal
     */
    @Override
    public Map<String, TraversalStatistics> getAveragesForInterval(long timestamp, long interval) throws RuntimeException {
        String avgDataSql = "select " + COLUMN_CONNECTION_ID + ", "
                + "count(" + COLUMN_CONNECTION_ID + ") as samples , "
                + "avg(" + COLUMN_TEMPORAL_MEAN_SPEED + ") as temporalMS_avg, "
                + "avg(" + COLUMN_SPATIAL_MEAN_SPEED + ") as spatialMS_avg, "
                + "avg(" + COLUMN_NAIVE_MEAN_SPEED + ") as naiveMS_avg, "
                + "avg(" + COLUMN_TRAVERSAL_TIME + ") as travTime_avg, "
                + "avg(" + COLUMN_RELATIVE_TRAFFIC_METRIC + ") as rtsm_avg "
                + "from " + TABLE_TRAVERSAL_METRICS + " "
                + "where "
                + COLUMN_TIME_STAMP + " > " + timestamp + " AND " + COLUMN_TIME_STAMP + " < " + (timestamp + interval) + " "
                + "group by " + COLUMN_CONNECTION_ID + ";";
        try (PreparedStatement statement = connection.prepareStatement(avgDataSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                Map<String, TraversalStatistics> results = new HashMap<>();
                while (rs.next()) {
                    TraversalStatistics traversalStatistics = new TraversalStatistics(
                            rs.getString(COLUMN_CONNECTION_ID),
                            timestamp,
                            rs.getInt("samples"),
                            rs.getDouble("temporalMS_avg"),
                            rs.getDouble("spatialMS_avg"),
                            rs.getDouble("naiveMS_avg"),
                            rs.getFloat("rtsm_avg"),
                            getSpeedPerformanceIndexFor(rs.getDouble("temporalMS_avg"), rs.getString(COLUMN_CONNECTION_ID)));
                    results.putIfAbsent(traversalStatistics.getConnectionId(), traversalStatistics);
                }
                return results;
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "STATISTICS COMPUTATION");
            return null;
        }
    }

    /**
     * Initializes the cache for the thresholds with data from the database file.
     */
    private void initializeCache() {
        String thresholdSql = "SELECT "
                + COLUMN_CONNECTION_ID + ", "
                + COLUMN_TEMPORAL_THRESHOLD + ", "
                + COLUMN_SPATIAL_THRESHOLD
                + " FROM "
                + TABLE_THRESHOLDS
                + " GROUP BY " + COLUMN_CONNECTION_ID
                + " HAVING "
                + "MAX(" + COLUMN_TIME_OF_INSERTION + ") = " + COLUMN_TIME_OF_INSERTION + ";";
        try (PreparedStatement statement = connection.prepareStatement(thresholdSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    thresholdCache.put(
                            rs.getString(COLUMN_CONNECTION_ID),
                            new Pair<>(rs.getDouble(COLUMN_TEMPORAL_THRESHOLD), rs.getDouble(COLUMN_SPATIAL_THRESHOLD))
                    );
                }
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "CACHE INITIALIZATION");
        }
    }

    /**
     * Computes the Speed Performance Index as speed over max allowed speed on the connection.
     *
     * @param speed        for which the index will be computed
     * @param connectionId ID of the connection
     * @return the Speed Performance Index as a value between 0 and 1
     */
    private Double getSpeedPerformanceIndexFor(Double speed, String connectionId) {
        return speed / networkDb.getConnection(connectionId).getMaxSpeedInMs();
    }

    /**
     * Checks if there is thresholds data for the current connection in the cache.
     *
     * @param connectionId for which to check
     * @return true if present, else false
     */
    @Override
    public boolean gotThresholdFor(String connectionId) {
        return thresholdCache.containsKey(connectionId);
    }

    @Override
    public String getStatisticsString() {
        String statisticsString = "Statistics for FCD Database:";
        statisticsString += System.lineSeparator() + "Record Amount: " + getRowAmount(TABLE_RECORDS);
        statisticsString += System.lineSeparator() + "Traversal Amount: " + getRowAmount(TABLE_TRAVERSAL_METRICS);
        statisticsString += System.lineSeparator() + "Threshold Amount: " + getRowAmount(TABLE_THRESHOLDS);
        statisticsString += System.lineSeparator() + "Connection Amount: " + getRowAmount(TABLE_CONNECTIONS);
        return statisticsString;
    }

    protected int getRowAmount(String table) {
        return getRowAmount(table, "");
    }

    protected int getRowAmount(String table, String condition) throws RuntimeException {
        String rowAmountSql = "SELECT COUNT(*) FROM " + table + " " + condition + ";";
        int rowAmount = 0;
        try (PreparedStatement statement = connection.prepareStatement(rowAmountSql)) {
            try (ResultSet rs = statement.executeQuery()) {
                rowAmount = rs.getInt(1);
            }
        } catch (SQLException exception) {
            logErrorAndThrowRuntimeException(exception, "ROW COUNT");
        }
        return rowAmount;
    }

    protected void logErrorAndThrowRuntimeException(Exception exception, String reason) throws RuntimeException {
        if (logger != null) {
            logger.error("FCD Database Error during {}: {}", reason, exception.getMessage());
        }
        throw new RuntimeException(exception);
    }
}
