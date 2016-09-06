package com.tvelykyy.filekafka;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceConnector.class);

    public static final String TOPIC_CONFIG = "topic";
    public static final String BASE_DIR_CONFIG = "baseDir";
    public static final String CURRENT_FILE_CONFIG = "currentFile";
    public static final String ROLLED_FILE_PATTERN_CONFIG = "rolledFilePattern";
    public static final String REAL_FILE_CONFIG = "realFileConfig";
    public static final String ROLLED_FILE_CONFIG = "rolledFileConfig";

    private String baseDir;
    private String currentFile;
    private String rolledFilePattern;
    private String topic;

    private DirectoryReader directoryReader;

    private FileMonitorThread monitorThread;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        baseDir = props.get(BASE_DIR_CONFIG);
        currentFile = props.get(CURRENT_FILE_CONFIG);
        rolledFilePattern = props.get(ROLLED_FILE_PATTERN_CONFIG);
        topic = props.get(TOPIC_CONFIG);

        directoryReader = new DirectoryReader(baseDir, currentFile, rolledFilePattern);

        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");

        monitorThread = new FileMonitorThread(context, 1000, baseDir, currentFile, rolledFilePattern);
        monitorThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        Map<String, String> mappedFiles = directoryReader.getMappedFiles();
        LOGGER.info("Got {} files to process", mappedFiles.size());

        mappedFiles.forEach((k,v)-> {
            Map<String, String> config = new HashMap<>();
            config.put(TOPIC_CONFIG, topic);
            config.put(REAL_FILE_CONFIG, k);
            config.put(ROLLED_FILE_CONFIG, v);

            configs.add(config);
        });

        return configs;
    }

    @Override
    public void stop() {
        monitorThread.shutdown();
        try {
            monitorThread.join(4000);
        } catch (InterruptedException e) {
            // Ignore, shouldn't be interrupted
        }
    }
}
