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

package com.dcaiti.mosaic.app.tse.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class OutputFileFactory {

    private static final String HADOOP_HOME_PROP = "hadoop.home.dir";

    private OutputFileFactory() {
    }

    public static OutputFile create(String path, String filename) {
        ensureHadoopHome();
        Path resolvedPath = Paths.get(path, filename).toAbsolutePath();
        try {
            Path parent = resolvedPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Configuration conf = new Configuration();
            conf.setClassLoader(OutputFileFactory.class.getClassLoader());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            return HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(resolvedPath.toString()), conf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void ensureHadoopHome() {
        // Hadoop emits warnings when HADOOP_HOME/hadoop.home.dir are missing; create a minimal stub once.
        if (System.getProperty(HADOOP_HOME_PROP) != null || System.getenv("HADOOP_HOME") != null) {
            return;
        }
        try {
            Path tempHome = Files.createTempDirectory("hadoop-home");
            Path binDir = tempHome.resolve("bin");
            Files.createDirectories(binDir);
            Files.createFile(binDir.resolve("hadoop"));
            System.setProperty(HADOOP_HOME_PROP, tempHome.toString());
        } catch (IOException ignored) {
            // If creation fails, fall back to default behavior.
        }
    }
}
