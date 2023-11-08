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

package com.dcaiti.mosaic.app.tse.processors;

import com.dcaiti.mosaic.app.fcd.data.FxdRecord;
import com.dcaiti.mosaic.app.fcd.messages.FxdUpdate;
import com.dcaiti.mosaic.app.tse.gson.TimeBasedProcessorTypeAdapterFactory;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.lang3.ClassUtils;

/**
 * {@link TimeBasedProcessor TimeBasedProcessors} perform everytime the configured {@link #triggerInterval} is reached.
 * Additionally, these processors allow independent processing of {@link UpdateT updates} and often hold their own representations.
 *
 * @param <RecordT> type of the used record
 * @param <UpdateT> type of the used update
 */
@JsonAdapter(TimeBasedProcessorTypeAdapterFactory.class)
public abstract class TimeBasedProcessor<RecordT extends FxdRecord, UpdateT extends FxdUpdate<RecordT>> implements FxdProcessor {

    /**
     * Sets the time interval at which the {@link #triggerEvent} function is being called.
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long triggerInterval = 30 * TIME.MINUTE;
    /**
     * Logger class that allows writing to application logs.
     */
    protected UnitLogger logger;
    private long nextTriggerTime;
    private long previousTriggerTime;

    @Override
    public void initialize(UnitLogger logger) {
        this.logger = logger;
        previousTriggerTime = 0;
        nextTriggerTime = 0;
    }

    public long getAndIncrementNextTriggerTime() {
        previousTriggerTime = nextTriggerTime;
        return nextTriggerTime += triggerInterval;
    }

    public long getPreviousTriggerTime() {
        return previousTriggerTime;
    }

    public abstract String getIdentifier();

    public abstract void handleUpdate(UpdateT update);

    /**
     * This method will handle the execution of the event.
     * And will be called by the kernel based on {@link #triggerInterval}.
     *
     * @param eventTime simulation time at event trigger
     */
    public abstract void triggerEvent(long eventTime);


    @SuppressWarnings("rawtypes")
    public static String createIdentifier(Class<? extends TimeBasedProcessor> processorClass) {
        return ClassUtils.getShortClassName(processorClass);
    }
}
