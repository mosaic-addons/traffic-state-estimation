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

import com.dcaiti.mosaic.app.tse.FxdKernel;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;

/**
 * Interface to be implemented by all processors to be handled by an {@link FxdKernel}.
 */
public interface FxdProcessor {

    void initialize(UnitLogger logger);

    /**
     * Finalize all processor computations and clear used memory.
     *
     * @param shutdownTime time of shutdown
     */
    void shutdown(long shutdownTime);
}
