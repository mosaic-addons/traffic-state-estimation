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

package com.dcaiti.mosaic.app.tse.config;

import com.dcaiti.mosaic.app.fxd.data.FxdRecord;
import com.dcaiti.mosaic.app.fxd.data.FxdTraversal;
import com.dcaiti.mosaic.app.fxd.messages.FxdUpdateMessage;
import com.dcaiti.mosaic.app.tse.FxdKernel;
import com.dcaiti.mosaic.app.tse.processors.MessageBasedProcessor;
import com.dcaiti.mosaic.app.tse.processors.TimeBasedProcessor;
import com.dcaiti.mosaic.app.tse.processors.TraversalBasedProcessor;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.gson.annotations.JsonAdapter;

import java.util.List;

public class CFxdReceiverApp<
        RecordT extends FxdRecord,
        TraversalT extends FxdTraversal<RecordT, TraversalT>,
        UpdateT extends FxdUpdateMessage<RecordT>> {
    /**
     * Within this period the {@link FxdKernel FxdKernel} will look for outdated/inactive sender trajectories and
     * remove them. [ns]
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long unitRemovalInterval = 30 * TIME.MINUTE;
    /**
     * Time after which a unit is treated as outdated.
     * This time is measured from the reception of newest received {@link FxdRecord FxdRecord} for each unit.
     * If a unit hasn't sent a message for this time, it will be removed with the next {@link #unitRemovalInterval}. [ns]
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long unitExpirationTime = 60 * TIME.MINUTE;
    /**
     * A list of configurable {@link TraversalBasedProcessor TraversalBasedProcessors}
     * which are managed by the {@link FxdKernel FxdKernel}.
     */
    public List<TraversalBasedProcessor<RecordT, TraversalT>> traversalBasedProcessors;
    /**
     * A list of configurable {@link TimeBasedProcessor TimeBasedProcessors} which are managed by the
     * {@link FxdKernel FxdKernel}.
     */
    public List<TimeBasedProcessor<RecordT, UpdateT>> timeBasedProcessors;
    /**
     * A list of configurable {@link MessageBasedProcessor MessageBasedProcessors}
     * which are managed by the {@link FxdKernel FxdKernel}.
     */
    public List<MessageBasedProcessor> messageBasedProcessors;
}
