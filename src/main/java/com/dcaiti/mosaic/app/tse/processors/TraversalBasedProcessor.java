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
import com.dcaiti.mosaic.app.fcd.data.FxdTraversal;
import com.dcaiti.mosaic.app.tse.FxdKernel;
import com.dcaiti.mosaic.app.tse.gson.TraversalBasedProcessorTypeAdapterFactory;

import com.google.gson.annotations.JsonAdapter;

/**
 * {@link FxdProcessor Processors} implementing this interface will perform an action whenever the
 * {@link FxdKernel FxdKernel} recognizes a new connection traversal of a vehicle.
 *
 * @param <RecordT>    concrete type of the record
 * @param <TraversalT> concrete type of the traversal
 */
@JsonAdapter(TraversalBasedProcessorTypeAdapterFactory.class)
public interface TraversalBasedProcessor<RecordT extends FxdRecord, TraversalT extends FxdTraversal<RecordT, TraversalT>>
        extends FxdProcessor {

    void onConnectionTraversal(String unitId, TraversalT traversal);
}
