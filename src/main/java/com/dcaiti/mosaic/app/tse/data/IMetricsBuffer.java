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

import java.util.List;

@SuppressWarnings("unused")
public interface IMetricsBuffer {

    void putOrAddSample(long timeStamp, ISampledVehicleSpeed sampledVehicleSpeed);

    List<Long> getBufferedTimeStamps();

    void clear();

    boolean isEmpty();

    long getOldestBufferedTimeStamp();

    long getNewestBufferedTimeStamp();

    List<String> getAllConnections(Long timeStamp);

    Double getSampledMeanSpeed(Long timeStamp, String connectionId);

    double getHarmonicMeanSpeed(Long timeStamp, String connectionId);

    int getSampleSize(Long timeStamp, String connectionId);

    boolean isConnectionOnlyPerceived(Long timeStamp, String connectionId);
}
