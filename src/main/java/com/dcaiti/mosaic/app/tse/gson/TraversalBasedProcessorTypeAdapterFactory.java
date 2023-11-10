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

package com.dcaiti.mosaic.app.tse.gson;

import com.dcaiti.mosaic.app.tse.processors.TraversalBasedProcessor;
import org.eclipse.mosaic.fed.application.ambassador.SimulationKernel;
import org.eclipse.mosaic.lib.util.gson.PackageSpecificTypeAdapter;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

public class TraversalBasedProcessorTypeAdapterFactory implements TypeAdapterFactory {

    private final static List<Package> SEARCH_PACKAGES = new ArrayList<>();

    public static void registerAdditionalPackageForSearch(Package name) {
        SEARCH_PACKAGES.add(name);
    }

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
        final PackageSpecificTypeAdapter<T> adapter = new PackageSpecificTypeAdapter<T>(this, gson)
                .withClassLoader(SimulationKernel.SimulationKernel.getClassLoader())
                .searchInPackageOfClass(TraversalBasedProcessor.class);
        for (Package searchPackage : SEARCH_PACKAGES) {
            adapter.searchInPackage(searchPackage);
        }
        return adapter.nullSafe();
    }
}
