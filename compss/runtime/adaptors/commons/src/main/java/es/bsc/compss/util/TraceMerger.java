/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TraceMerger {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Info used for matching sync events
    private static final Integer SYNC_TYPE = 8_000_666;
    private static final String SYNC_REGEX = "(^\\d+:\\d+:\\d+):(\\d+):(\\d+):(\\d+).*:" + SYNC_TYPE + ":(\\d+)";
    private static final Pattern SYNC_PATTERN = Pattern.compile(SYNC_REGEX);
    // Selectors for replace Pattern
    private static final Integer R_ID_INDEX = 1;
    private static final Integer TIMESTAMP_INDEX = 4;
    private static final Integer WORKER_ID_INDEX = 2;

    // could be wrong this regex (designed for matching tasks not workers)
    private static final String WORKER_THREAD_INFO_REGEX = "(^\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(.*)";
    private static final Pattern WORKER_THREAD_INFO_PATTERN = Pattern.compile(WORKER_THREAD_INFO_REGEX);
    private static final Integer STATE_TYPE = 1;
    private static final Integer WORKER_THREAD_ID = 2;
    private static final Integer WORKER_TIMESTAMP = 6;
    private static final Integer WORKER_LINE_INFO = 7;

    private static final String MASTER_TRACE_SUFFIX = "_compss_trace_";
    private static final String TRACE_EXTENSION = ".prv";
    private static final String WORKER_TRACE_SUFFIX = "_python_trace" + TRACE_EXTENSION;
    private static final String TRACE_SUBDIR = "trace";
    private static final String WORKER_SUBDIR = "python";

    private static String workingDir;

    private final File masterTrace;
    private final File[] workersTraces;
    private final String masterTracePath;
    private final String[] workersTracePath;
    private final PrintWriter masterWriter;


    private class LineInfo {

        private final String resourceId;
        private final Long timestamp;


        public LineInfo(String resourceID, Long timestamp) {
            this.resourceId = resourceID;
            this.timestamp = timestamp;
        }

        public String getResourceId() {
            return this.resourceId;
        }

        public Long getTimestamp() {
            return this.timestamp;
        }
    }


    /**
     * Trace Merger constructor.
     * 
     * @param workingDir Working directory
     * @param appName Application name
     * @throws IOException Error managing files
     */
    public TraceMerger(String workingDir, String appName) throws IOException {
        // Init master trace information
        final String traceNamePrefix = appName + MASTER_TRACE_SUFFIX;
        final File masterF = new File(workingDir + File.separator + TRACE_SUBDIR);
        final File[] matchingMasterFiles = masterF.listFiles(
            (File dir, String name) -> name.startsWith(traceNamePrefix) && name.endsWith(TRACE_EXTENSION));

        if (matchingMasterFiles == null || matchingMasterFiles.length < 1) {
            throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + TRACE_EXTENSION + " not found.");
        } else {
            this.masterTrace = matchingMasterFiles[0];
            this.masterTracePath = this.masterTrace.getAbsolutePath();
            if (matchingMasterFiles.length > 1) {
                LOGGER.warn("Found more than one master trace, using " + this.masterTrace + " to merge.");
            }
        }

        // Init workers traces information
        TraceMerger.workingDir = workingDir;
        final File workerF = new File(workingDir + File.separator + TRACE_SUBDIR + File.separator + WORKER_SUBDIR);
        File[] matchingWorkerFiles = workerF.listFiles((File dir, String name) -> name.endsWith(WORKER_TRACE_SUFFIX));

        if (matchingWorkerFiles == null) {
            throw new FileNotFoundException("No workers traces to merge found.");
        } else {
            this.workersTraces = matchingWorkerFiles;
        }

        this.workersTracePath = new String[this.workersTraces.length];
        for (int i = 0; i < this.workersTracePath.length; ++i) {
            this.workersTracePath[i] = this.workersTraces[i].getAbsolutePath();
        }

        // Initialize the writer for the final master trace
        this.masterWriter = new PrintWriter(new FileWriter(this.masterTracePath, true));

        LOGGER.debug("Trace's merger initialization successful");
    }

    /**
     * Merge traces.
     * 
     * @throws IOException Error managing traces
     */
    public void merge() throws IOException {
        LOGGER.debug("Parsing master sync events");
        Map<Integer, List<LineInfo>> masterSyncEvents = getSyncEvents(this.masterTracePath, -1);

        LOGGER.debug("Merging task traces into master which contains " + masterSyncEvents.size() + " lines.");
        for (File workerFile : this.workersTraces) {
            LOGGER.debug("Merging worker " + workerFile);
            String workerFileName = workerFile.getName();
            String wID = "";

            for (int i = 0; workerFileName.charAt(i) != '_'; ++i) {
                wID += workerFileName.charAt(i);
            }

            Integer workerID = Integer.parseInt(wID);
            workerID++; // first worker is resource number 2

            List<String> cleanLines = getWorkerEvents(workerFile);
            Map<Integer, List<LineInfo>> workerSyncEvents = getSyncEvents(workerFile.getPath(), workerID);

            writeWorkerEvents(masterSyncEvents, workerSyncEvents, cleanLines, workerID);
        }
        this.masterWriter.close();

        LOGGER.debug("Merging finished.");

        if (!DEBUG) {
            String workerFolder = workingDir + File.separator + TRACE_SUBDIR + File.separator + WORKER_SUBDIR;
            LOGGER.debug("Removing folder " + workerFolder);
            try {
                removeFolder(workerFolder);
            } catch (IOException ioe) {
                LOGGER.warn("Could not remove python temporal tracing folder" + ioe.toString());
            }
        }
    }

    private void removeFolder(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

    private void add(Map<Integer, List<LineInfo>> map, Integer key, LineInfo newValue) {
        List<LineInfo> currentValue = map.computeIfAbsent(key, k -> new ArrayList<>());
        currentValue.add(newValue);
    }

    private Map<Integer, List<LineInfo>> getSyncEvents(String tracePath, Integer workerID) throws IOException {
        Map<Integer, List<LineInfo>> idToSyncInfo = new HashMap<>();
        try (FileInputStream inputStream = new FileInputStream(tracePath);
                Scanner sc = new Scanner(inputStream, "UTF-8")) {

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                Matcher m = SYNC_PATTERN.matcher(line);
                if (m.find()) {
                    Integer wID = (workerID == -1) ? Integer.parseInt(m.group(WORKER_ID_INDEX)) : workerID;
                    String resourceID = m.group(R_ID_INDEX);
                    Long timestamp = Long.parseLong(m.group(TIMESTAMP_INDEX));

                    add(idToSyncInfo, wID, new LineInfo(resourceID, timestamp));
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } // Exceptions are raised automatically, we add the try clause to automatically close the streams

        return idToSyncInfo;
    }

    private List<String> getWorkerEvents(File worker) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(worker.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 1; // Remove header
        int endIndex = lines.size() - 1;

        return lines.subList(startIndex, endIndex);
    }

    private void writeWorkerEvents(Map<Integer, List<LineInfo>> masterSyncEvents,
            Map<Integer, List<LineInfo>> workerSyncEvents, List<String> eventsLine, Integer workerID) {

        LOGGER.debug("Writing " + eventsLine.size() + " lines from worker " + workerID);
        LineInfo workerHeader = getWorkerInfo(masterSyncEvents.get(workerID), workerSyncEvents.get(workerID));

        for (String line : eventsLine) {
            String newEvent = updateEvent(workerHeader, line, workerID);
            this.masterWriter.println(newEvent);
        }
    }

    private String updateEvent(LineInfo workerHeader, String line, Integer workerID) {
        Matcher taskMatcher = WORKER_THREAD_INFO_PATTERN.matcher(line);
        String newLine = "";
        if (taskMatcher.find()) {
            Integer threadID = Integer.parseInt(taskMatcher.group(WORKER_THREAD_ID));
            Integer stateID = Integer.parseInt(taskMatcher.group(STATE_TYPE));
            String eventHeader = stateID + ":" + threadID + ":1:" + workerID + ":" + threadID;
            Long timestamp = workerHeader.getTimestamp() + Long.parseLong(taskMatcher.group(WORKER_TIMESTAMP));
            String lineInfo = taskMatcher.group(WORKER_LINE_INFO);
            newLine = eventHeader + ":" + timestamp + ":" + lineInfo;
        }

        return newLine;
    }

    private LineInfo getWorkerInfo(List<LineInfo> masterSyncEvents, List<LineInfo> workerSyncEvents) {
        LineInfo javaStart = masterSyncEvents.get(0);
        LineInfo javaEnd = masterSyncEvents.get(1);

        LineInfo workerStart = workerSyncEvents.get(0);
        LineInfo workerEnd = workerSyncEvents.get(1);

        Long javaTime = Math.abs(javaStart.getTimestamp() - javaEnd.getTimestamp());
        Long workerTime = Math.abs(workerStart.getTimestamp() - workerEnd.getTimestamp());

        Long overhead = (javaTime - workerTime) / 2;

        return new LineInfo(javaStart.getResourceId(), javaStart.getTimestamp() + overhead);
    }

}
