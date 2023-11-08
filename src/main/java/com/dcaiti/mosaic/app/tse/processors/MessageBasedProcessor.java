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

import com.dcaiti.mosaic.app.tse.gson.MessageBasedProcessorTypeAdapterFactory;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;

import com.google.gson.annotations.JsonAdapter;

/**
 * {@link FxdProcessor Processors} implementing this interface can be used to perform an action whenever a {@link V2xMessage} of a specific
 * type is received.
 */
@JsonAdapter(MessageBasedProcessorTypeAdapterFactory.class)
public interface MessageBasedProcessor extends FxdProcessor {

    /**
     * Handles the reception of {@link V2xMessage V2xMessages} that clear the {@link #isInstanceOfMessage} check.
     * Furthermore, this method allows building a response, which will be transmitted to the sender of the
     * original message.
     *
     * @param message         the message container including a {@link V2xMessage} and additional information
     * @param responseRouting {@link MessageRouting} pointing back to the original sender (Required for building the response)
     * @return A response in the form of a {@link V2xMessage}
     */
    V2xMessage handleReceivedMessage(ReceivedV2xMessage message, MessageRouting responseRouting);

    /**
     * This method is used to validate that a {@link MessageBasedProcessor} can handle the specified message.
     * Implementations will usually look like {@code message.getMessage() instanceof <message-type>}
     *
     * @param message the message to check
     * @return a flag indicating whether the message can be processed or not
     */
    boolean isInstanceOfMessage(ReceivedV2xMessage message);
}
