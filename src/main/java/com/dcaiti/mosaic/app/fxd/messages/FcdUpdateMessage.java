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

package com.dcaiti.mosaic.app.fxd.messages;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serial;
import java.util.SortedMap;

/**
 * This class is used to transmit a package of {@link FcdRecord FcdRecords} to the server.
 */
public class FcdUpdateMessage extends FxdUpdateMessage<FcdRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Constructor for the {@link FcdUpdateMessage} class.
     *
     * @param routing    to the server
     * @param timestamp  last timestamp of records
     * @param fcdRecords a queue of FcdRecords
     */
    private FcdUpdateMessage(MessageRouting routing, long timestamp, SortedMap<Long, FcdRecord> fcdRecords, boolean isFinal) {
        super(routing, timestamp, fcdRecords, isFinal);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .toString();
    }

    public static class FcdUpdateMessageBuilder extends AbstractUpdateMessageBuilder<FcdUpdateMessageBuilder, FcdRecord, FcdUpdateMessage> {
        public FcdUpdateMessageBuilder(MessageRouting messageRouting, long timestamp) {
            super(messageRouting, timestamp);
        }

        @Override
        protected FcdUpdateMessageBuilder getThis() {
            return this;
        }

        @Override
        protected FcdUpdateMessage internalBuild() {
            return new FcdUpdateMessage(messageRouting, timestamp, records, isFinal);
        }
    }
}
