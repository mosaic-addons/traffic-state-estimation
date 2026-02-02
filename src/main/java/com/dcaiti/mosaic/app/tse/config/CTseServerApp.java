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
 * which mainly includes configuration parameters for data storage (Parquet output).
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
     * Set to {@code true}, if data should be kept and not reset before every simulation.
     * Note: For Parquet storage, this is currently not implemented (files will be overwritten).
     */
    public boolean isPersistent = false;
    /**
     * Optional path to the Parquet output directory. If no path is configured,
     * files will be created in the application's log directory.
     */
    public String parquetOutputPath = null;
    /**
     * @deprecated Use {@link #parquetOutputPath} instead. Kept for backward compatibility.
     */
    @Deprecated
    public String databasePath = null;
    /**
     * @deprecated No longer used with Parquet storage. Kept for backward compatibility.
     */
    @Deprecated
    public String databaseFileName = null;

    @Override
    public String toString() {
        return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("isPersistent", isPersistent)
                .append("parquetOutputPath", parquetOutputPath)
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
