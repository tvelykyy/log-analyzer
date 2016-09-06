/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.tvelykyy.filekafka;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class FileMonitorThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMonitorThread.class);

    private final ConnectorContext context;
    private final CountDownLatch shutdownLatch;
    private final long pollMs;
    private int filesCount;
    private DirectoryReader directoryReader;

    public FileMonitorThread(ConnectorContext context, long pollMs, String baseDir, String currentFile, String rolledFilesPattern)  {
        this.context = context;
        this.shutdownLatch = new CountDownLatch(1);
        this.pollMs = pollMs;
        this.directoryReader = new DirectoryReader(baseDir, currentFile, rolledFilesPattern);
        this.filesCount = directoryReader.getMappedFiles().size();
        LOGGER.info("Started monitor thread.");
    }

    @Override
    public void run() {
        while (shutdownLatch.getCount() > 0) {
            if (isNewFiles()) {
                LOGGER.info("Found new files. Reconfiguring.");
                context.requestTaskReconfiguration();
            }
            LOGGER.info("No new files.");

            try {
                boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
                if (shuttingDown) {
                    return;
                }
          } catch (InterruptedException e) {
            LOGGER.error("Unexpected InterruptedException, ignoring: ", e);
          }
        }
    }

    public void shutdown() {
        shutdownLatch.countDown();
    }

        // Update tables and return true if the
    private boolean isNewFiles() {
        //TODO possibility: one file removed, one file added
        int currentFilesCount = directoryReader.getMappedFiles().size();
        if (filesCount != currentFilesCount) {
            filesCount = currentFilesCount;
            return true;
        }
        return false;
    }
}
