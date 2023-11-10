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

package com.dcaiti.mosaic.app.tse.data;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents a Traversal over one connection.
 */
public class TraversalStatistics {
    /**
     * ID of the connection that was traversed.
     */
    private final String connectionId;
    /**
     * Timestamp when this traversal was finished.
     */
    private final Long timestamp;
    /**
     * The mean speed over the entire traversals travel time.
     */
    private final Double temporalMeanSpeed;
    /**
     * The mean speed averaged over multiple equidistant points along the connection.
     */
    private final Double spatialMeanSpeed;
    /**
     * To identify unique traversals, is derived from the ROW_ID in the DB.
     */
    private Integer traversalId;
    /**
     * If this is an averaged traversal, it holds the sample size.
     */
    private Integer samples;
    /**
     * A mean speed that is simply the average speed over all FcdRecords for this traversal.
     */
    private Double naiveMeanSpeed;
    /**
     * The main relative traffic metric as proposed by Yoon et al.
     */
    private Float relativeTrafficStatusMetric;
    /**
     * The SpeedPerformanceIndex as temporal mean speed over max allowed speed of the connection.
     */
    private Double speedPerformanceIndex;

    /**
     * Constructor setting only the mean speeds for a single traversal.
     */
    public TraversalStatistics(String connectionId, int traversalId, Long timestamp, Double temporalMeanSpeed, Double spatialMeanSpeed) {
        this.connectionId = connectionId;
        this.traversalId = traversalId;
        this.timestamp = timestamp;
        this.temporalMeanSpeed = temporalMeanSpeed;
        this.spatialMeanSpeed = spatialMeanSpeed;
    }

    /**
     * Constructor setting all metrics for a single traversal.
     */
    public TraversalStatistics(String connectionId, int traversalId, Long timestamp, Double temporalMeanSpeed,
                               Double spatialMeanSpeed, Float relativeTrafficStatusMetric, Double speedPerformanceIndex) {
        this.connectionId = connectionId;
        this.traversalId = traversalId;
        this.timestamp = timestamp;
        this.temporalMeanSpeed = temporalMeanSpeed;
        this.spatialMeanSpeed = spatialMeanSpeed;
        this.relativeTrafficStatusMetric = relativeTrafficStatusMetric;
        this.speedPerformanceIndex = speedPerformanceIndex;
    }

    /**
     * Constructor for setting all metrics for aggregated traversal data of a single connection.
     */
    public TraversalStatistics(String connectionId, Long timestamp, Integer samples, Double temporalMeanSpeed,
                               Double spatialMeanSpeed, Double naiveMeanSpeed, Float relativeTrafficStatusMetric, Double speedPerformanceIndex) {
        this.connectionId = connectionId;
        this.samples = samples;
        this.timestamp = timestamp;
        this.temporalMeanSpeed = temporalMeanSpeed;
        this.spatialMeanSpeed = spatialMeanSpeed;
        this.naiveMeanSpeed = naiveMeanSpeed;
        this.relativeTrafficStatusMetric = relativeTrafficStatusMetric;
        this.speedPerformanceIndex = speedPerformanceIndex;
    }

    public Integer getSamples() {
        return samples;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public Integer getTraversalId() {
        return traversalId;
    }

    public Long getTimestamp() {
        return timestamp;
    }


    public Double getTemporalMeanSpeed() {
        return temporalMeanSpeed;
    }

    public Double getSpatialMeanSpeed() {
        return spatialMeanSpeed;
    }

    public Double getNaiveMeanSpeed() {
        return naiveMeanSpeed;
    }

    public Double getSpeedPerformanceIndex() {
        return speedPerformanceIndex;
    }

    public int getSize() {
        return connectionId.length() + 4 + 8 + 8 + 8 + 4;
    }

    public Float getRelativeTrafficStatusMetric() {
        return relativeTrafficStatusMetric;
    }

    public void setRelativeTrafficStatusMetric(Float relativeTrafficStatusMetric) {
        this.relativeTrafficStatusMetric = relativeTrafficStatusMetric;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("connectionId", connectionId)
                .append("traversalId", traversalId)
                .append("timestampOfLastRecord", timestamp)
                .append("temporalMeanSpeed", temporalMeanSpeed)
                .append("spatialMeanSpeed", spatialMeanSpeed)
                .append("relativeTrafficStatusMetric", relativeTrafficStatusMetric)
                .toString();
    }
}
