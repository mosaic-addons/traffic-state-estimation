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

package com.dcaiti.mosaic.app.fcd.config;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class CFxdTransmitterApp {

    /**
     * The id of the unit that shall receive the packages.
     */
    public String receiverId = "server_0";

    /**
     * The interval in which data shall be collected. [ns]
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public Long collectionInterval = TIME.SECOND;

    /**
     * The interval in which data packages shall be transmitted. [ns]
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public Long transmissionInterval = 30 * TIME.SECOND;

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .append("receiverId", receiverId)
                .append("collectionInterval", collectionInterval)
                .append("transmissionInterval", transmissionInterval)
                .toString();
    }
}
