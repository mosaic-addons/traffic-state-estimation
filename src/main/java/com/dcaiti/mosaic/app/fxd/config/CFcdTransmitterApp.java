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

package com.dcaiti.mosaic.app.fxd.config;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This extension of {@link CFxdTransmitterApp} only adds additional parameters regarding the perception capabilities of vehicles.
 */
public class CFcdTransmitterApp extends CFxdTransmitterApp {

    /**
     * Enables the inclusion of perceived vehicles within the {@link com.dcaiti.mosaic.app.fxd.data.FcdRecord FcdRecords}.
     */
    public boolean includePerceptionData = false;
    /**
     * The viewing range to be used for equipped vehicles.
     * Note: this has no effect if {@link #includePerceptionData} is set to {@code false}
     */
    public double viewingRange = 100;
    /**
     * The viewing angle to be used for equipped vehicles.
     * Note: this has no effect if {@link #includePerceptionData} is set to {@code false}
     */
    public double viewingAngle = 360;
    /**
     * Flag indicating whether wall occlusion should be enabled.
     */
    public boolean enableWallOcclusion = false;
    /**
     * Flag indicating whether vehicles will occlude other vehicles using a bounding box-based occlusion evaluation.
     */
    public boolean enableVehicleOcclusion = false;

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("includePerceptionData", includePerceptionData)
                .append("viewingRange", viewingRange)
                .append("viewingAngle", viewingAngle)
                .append("enableWallOcclusion", enableWallOcclusion)
                .append("enableVehicleOcclusion", enableVehicleOcclusion)
                .toString();
    }
}
