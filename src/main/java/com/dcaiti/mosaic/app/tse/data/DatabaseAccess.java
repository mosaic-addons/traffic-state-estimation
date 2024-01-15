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

package com.dcaiti.mosaic.app.tse.data;

import com.dcaiti.mosaic.app.tse.TseKernel;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import org.eclipse.mosaic.lib.database.Database;

/**
 * An interface to be implemented by {@link com.dcaiti.mosaic.app.tse.processors.FxdProcessor processors} that need access to the
 * {@link Database Network Database} and/or the {@link FcdDataStorage}.
 * The respective method will be called by the {@link TseKernel}.
 */
public interface DatabaseAccess {
    /**
     * Method giving processors access to the {@link Database Network Database} and the {@link FcdDataStorage}.
     *
     * @param networkDatabase   reference to the {@link Database Network Database}
     * @param fcdDataStorage reference to the {@link FcdDataStorage}
     */
    void withDataStorage(Database networkDatabase, FcdDataStorage fcdDataStorage);
}
