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

package com.dcaiti.mosaic.app.fxd.data;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * An {@link FcdTraversal} is a data object storing all records on a connection for a single vehicle trajectory.
 * Additionally, the last record on the previous connection and the first record on the following connection of the vehicle are added
 * for interpolation purposes in the {@link com.dcaiti.mosaic.app.tse.processors.SpatioTemporalProcessor SpatioTemporalProcessor}.
 */
public class FcdTraversal extends FxdTraversal<FcdRecord, FcdTraversal> {
    public FcdTraversal(String connectionId, List<FcdRecord> traversal, FcdRecord previousRecord, FcdRecord followingRecord) {
        super(connectionId, traversal, previousRecord, followingRecord);
    }

    @Override
    public FcdTraversal copy() {
        return new FcdTraversal(connectionId, Lists.newArrayList(traversal), previousRecord, followingRecord);
    }
}
