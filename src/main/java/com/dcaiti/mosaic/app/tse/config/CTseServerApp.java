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

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.fxd.data.FcdTraversal;
import com.dcaiti.mosaic.app.fxd.messages.FcdUpdateMessage;
import com.dcaiti.mosaic.app.tse.TseServerApp;
import com.dcaiti.mosaic.app.tse.gson.TimeBasedProcessorTypeAdapterFactory;
import com.dcaiti.mosaic.app.tse.gson.TraversalBasedProcessorTypeAdapterFactory;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.processors.SpatioTemporalProcessor;
import com.dcaiti.mosaic.app.tse.processors.ThresholdProcessor;

import com.google.gson.Gson;
import com.google.gson.ToNumberPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * An extension of {@link CFxdReceiverApp}, holding the configuration of the {@link TseServerApp},
 * which manly includes configuration parameters for the database.
 * Additionally, the {@link TypeAdapterFactory TypeAdapterFactories} for the relevant
 * {@link com.dcaiti.mosaic.app.tse.processors.FxdProcessor processors} is added.
 */
@JsonAdapter(CTseServerApp.TypeExtendingTypeAdapterFactory.class)
public class CTseServerApp extends CFxdReceiverApp<FcdRecord, FcdTraversal, FcdUpdateMessage> {

    /**
     * Implementation to be used for the {@link FcdDataStorage}.
     */
    public FcdDataStorage fcdDataStorage = null;
    /**
     * Set to {@code true}, if the database should be kept and not reset before every simulation.
     * Can be useful to be used as a threshold from a prior long simulation in a shorter new simulation on the same network.
     */
    public boolean isPersistent = false;
    /**
     * Optional path to the database file. If no path is configured,
     * the database will be created in the application directory.
     */
    public String databasePath = null;
    /**
     * Optional path to the db file. If there is none, one will be created.
     */
    public String databaseFileName = null;
    /**
     * If {@code true}, all {@link FcdRecord FcdRecords} will be stored to the database.
     * Can be turned off, to safe storage and compute time, as metrics are computed from memory, not from records in the database.
     */
    public boolean storeRawFcd = false;

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("isPersistent", isPersistent)
                .append("databasePath", databasePath)
                .append("databaseFileName", databaseFileName)
                .toString();
    }

    /**
     * This {@link TypeAdapterFactory} is used to register additional package search information
     * to the {@link TimeBasedProcessorTypeAdapterFactory} and {@link TraversalBasedProcessorTypeAdapterFactory}.
     */
    static class TypeExtendingTypeAdapterFactory implements TypeAdapterFactory {

        static {
            TimeBasedProcessorTypeAdapterFactory.registerAdditionalPackageForSearch(ThresholdProcessor.class.getPackage());
            TraversalBasedProcessorTypeAdapterFactory.registerAdditionalPackageForSearch(SpatioTemporalProcessor.class.getPackage());
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
            return ObjectTypeAdapter.getFactory(ToNumberPolicy.DOUBLE).create(gson, typeToken);
        }
    }
}
