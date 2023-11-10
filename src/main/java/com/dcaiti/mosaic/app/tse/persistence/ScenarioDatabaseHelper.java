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

package com.dcaiti.mosaic.app.tse.persistence;

import org.eclipse.mosaic.fed.application.ambassador.SimulationKernel;
import org.eclipse.mosaic.fed.application.app.api.os.OperatingSystem;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.database.road.Connection;
import org.eclipse.mosaic.lib.database.road.Node;
import org.eclipse.mosaic.lib.routing.database.DatabaseRouting;

import java.io.File;
import java.util.List;

/**
 * This helper class contains static methods to set up a MOSAIC scenario database object from the database file.
 * Also holds a function to calculate the length of {@link Connection connections} by its' {@link Node nodes}.
 */
public final class ScenarioDatabaseHelper {

    /**
     * Creates a Database object from the Mosaic DB file.
     *
     * @param os operating system
     * @return The Mosaic Database.
     */
    public static Database getNetworkDbFromFile(OperatingSystem os) {
        if (SimulationKernel.SimulationKernel.getCentralNavigationComponent().getRouting() instanceof DatabaseRouting) {
            return ((DatabaseRouting) SimulationKernel.SimulationKernel.getCentralNavigationComponent().getRouting()).getScenarioDatabase();
        }
        File[] dbFiles = os.getConfigurationPath().listFiles((f, n) -> n.endsWith(".db") || n.endsWith(".sqlite"));
        if (dbFiles != null && dbFiles.length > 0) {
            return Database.loadFromFile(dbFiles[0]);
        }
        throw new RuntimeException("Cant find network database. Searching in " + os.getConfigurationPath().getAbsolutePath());
    }


    /**
     * Calculates the length of a {@link Connection} by adding up the distance between its {@link Node Nodes}.
     * This is required as the length stored in DB is not always accurate.
     *
     * @param connection for which the length needs to be calculated
     * @return length of connection
     */
    public static double calcLengthByNodes(Connection connection) {
        List<Node> nodes = connection.getNodes();
        double length = 0.0;
        Node prev = null;
        for (Node curr : nodes) {
            if (prev != null) {
                length += prev.getPosition().distanceTo(curr.getPosition());
            }
            prev = curr;
        }
        return length;
    }

}
