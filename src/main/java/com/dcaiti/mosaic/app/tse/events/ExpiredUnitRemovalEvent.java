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

package com.dcaiti.mosaic.app.tse.events;

import com.dcaiti.mosaic.app.tse.config.CFxdReceiverApp;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.lib.util.scheduling.EventProcessor;

import javax.annotation.Nonnull;

/**
 * This event is scheduled within a fixed configured interval (see {@link CFxdReceiverApp CFxdReceiverApp})
 * and will trigger the removal of all outdated units.
 */
public class ExpiredUnitRemovalEvent extends Event {
    public ExpiredUnitRemovalEvent(long time, @Nonnull EventProcessor processor) {
        super(time, processor);
    }
}
