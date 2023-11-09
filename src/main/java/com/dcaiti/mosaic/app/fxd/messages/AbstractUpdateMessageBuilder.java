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
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This abstract builder class is to be used for the building of {@link FxdUpdateMessage}-implementations. All common fields are contained
 * within this class. Additional fields can be added by extending this class.
 *
 * @param <BuilderT> concrete type of the builder
 * @param <RecordT>  concrete type of the record
 * @param <UpdateT>  concrete type of the update
 */
public abstract class AbstractUpdateMessageBuilder<
        BuilderT extends FxdUpdateMessage.FxdUpdateMessageBuilder<BuilderT, RecordT, UpdateT>,
        RecordT extends FxdRecord,
        UpdateT extends FxdUpdateMessage<RecordT>
        >
        implements FxdUpdateMessage.FxdUpdateMessageBuilder<BuilderT, RecordT, UpdateT> {
    protected final MessageRouting messageRouting;
    protected final long timestamp;
    protected final SortedMap<Long, RecordT> records = new TreeMap<>();
    protected boolean isFinal = false;

    public AbstractUpdateMessageBuilder(MessageRouting messageRouting, long timestamp) {
        this.messageRouting = messageRouting;
        this.timestamp = timestamp;
    }

    @Override
    public BuilderT addRecords(SortedMap<Long, RecordT> records) {
        this.records.putAll(records);
        return getThis();
    }

    @Override
    public BuilderT isFinal() {
        isFinal = true;
        return getThis();
    }

    protected abstract BuilderT getThis();

    protected abstract UpdateT internalBuild();

    public UpdateT build() {
        return internalBuild();
    }

}
