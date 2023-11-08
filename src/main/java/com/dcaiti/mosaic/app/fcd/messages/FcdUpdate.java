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

package com.dcaiti.mosaic.app.fcd.messages;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import com.dcaiti.mosaic.app.fcd.data.FcdRecord;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.SortedMap;

/**
 * This class is used to transmit a package of {@link FcdRecord FcdRecords} to the server.
 */
public class FcdUpdate extends FxdUpdate<FcdRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor for the {@link FcdUpdate} class.
     *
     * @param routing    to the server
     * @param timestamp  last timestamp of records
     * @param fcdRecords a queue of FcdRecords
     */
    private FcdUpdate(MessageRouting routing, long timestamp, SortedMap<Long, FcdRecord> fcdRecords, boolean isFinal) {
        super(routing, timestamp, fcdRecords, isFinal);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .toString();
    }

    public static class Builder extends AbstractUpdateBuilder<Builder, FcdRecord, FcdUpdate> {
        public Builder(MessageRouting messageRouting, long timestamp) {
            super(messageRouting, timestamp);
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected FcdUpdate internalBuild() {
            return new FcdUpdate(messageRouting, timestamp, records, isFinal);
        }
    }
}
