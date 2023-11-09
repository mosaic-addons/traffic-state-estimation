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

import com.dcaiti.mosaic.app.fxd.data.FxdRecord;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

import com.google.common.collect.Maps;

import java.util.SortedMap;
import javax.annotation.Nonnull;

/**
 * This extension {@link V2xMessage} is used to transmit {@link FxdRecord FxdRecords} from vehicles to a server. It is configured by
 * the {@link RecordT record type}, which extensions of this class have to specify.
 *
 * @param <RecordT> type of record that is transmitted by the extension of this class
 */
public abstract class FxdUpdateMessage<RecordT extends FxdRecord> extends V2xMessage {

    private final long timeStamp;
    private final SortedMap<Long, RecordT> records;
    private final boolean isFinal;

    protected FxdUpdateMessage(MessageRouting messageRouting, long timeStamp, SortedMap<Long, RecordT> records, boolean isFinal) {
        super(messageRouting);
        this.timeStamp = timeStamp;
        this.records = records;
        this.isFinal = isFinal;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public final SortedMap<Long, RecordT> getRecords() {
        // always return copy of records, in case list is processed by multiple processors
        return Maps.newTreeMap(records);
    }

    public boolean isFinal() {
        return isFinal;
    }

    @Nonnull
    @Override
    public EncodedPayload getPayLoad() {
        return new EncodedPayload(calculateMessageLength());
    }

    /**
     * Method that estimates the length of an average {@link FxdUpdateMessage UpdateMessage} adding the baseline length of required fields with
     * the length of the specialized {@link FxdUpdateMessage FxdUpdateMessages}.
     */
    public long calculateMessageLength() {
        return 10 // "header size"
                + 8 // for time stamp
                + 1 // for isFinal flag
                + getRecords().values().stream().mapToLong(FxdRecord::calculateRecordSize).sum(); // size for each record
    }

    /**
     * Interface for the builder of {@link FxdUpdateMessage FxdUpdateMessages}.
     *
     * @param <BuilderT> concrete type of the builder
     * @param <RecordT>  concrete type of the records
     * @param <UpdateT>  concrete type of the updates being built
     */
    interface FxdUpdateMessageBuilder<
            BuilderT extends FxdUpdateMessageBuilder<BuilderT, RecordT, UpdateT>,
            RecordT extends FxdRecord,
            UpdateT extends FxdUpdateMessage<RecordT>> {

        BuilderT addRecords(SortedMap<Long, RecordT> records);

        BuilderT isFinal();
    }

}
