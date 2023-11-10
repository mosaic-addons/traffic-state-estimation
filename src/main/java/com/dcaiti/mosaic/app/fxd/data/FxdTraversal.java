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

import java.util.List;
import javax.annotation.Nullable;

/**
 * An {@link FxdTraversal} contains a list of all {@link RecordT Records} sampled on a connection.
 * The length of this list is highly dependent on sampling rates, connection lengths and unit speeds. Additionally,
 * the {@link #previousRecord} and {@link #followingRecord} are added, which can be applied for interpolation purposes.
 */
public abstract class FxdTraversal<RecordT extends FxdRecord, TraversalT extends FxdTraversal<RecordT, TraversalT>> {

    protected final String connectionId;
    protected final List<RecordT> traversal;
    /**
     * Last record of previous traversal.
     */
    protected final RecordT previousRecord;
    /**
     * First record of previous following traversal.
     */
    protected final RecordT followingRecord;

    /**
     * Constructor for an {@link FxdTraversal}.
     */
    public FxdTraversal(String connectionId, List<RecordT> traversal, @Nullable RecordT previousRecord, @Nullable RecordT followingRecord) {
        this.connectionId = connectionId;
        this.traversal = traversal;
        this.previousRecord = previousRecord;
        this.followingRecord = followingRecord;
    }

    /**
     * Returns the id of the traversed connection.
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Returns a list of all {@link RecordT Records} on the traversed connection.
     */
    public List<RecordT> getTraversal() {
        return traversal;
    }

    /**
     * Returns the last record of the previous traversal.
     * Note: {@link RecordT#getConnectionId()} will be different from {@link #getConnectionId()}
     */
    @Nullable
    public RecordT getPreviousRecord() {
        return previousRecord;
    }

    /**
     * Returns the first record of the following traversal.
     * Note: {@link RecordT#getConnectionId()} will be different from {@link #getConnectionId()}
     */
    @Nullable
    public RecordT getFollowingRecord() {
        return followingRecord;
    }

    /**
     * Method to create a deep copy of the {@link FxdTraversal}, needs to be implemented by child classes.
     */
    public abstract TraversalT copy();
}
