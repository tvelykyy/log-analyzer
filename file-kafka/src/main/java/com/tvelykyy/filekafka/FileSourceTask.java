package com.tvelykyy.filekafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FileSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceTask.class);
    private static final int MAX_FILE_SIZE_BYTES = 1024 * 1024;

    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String realFile;
    private String rolledFile;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;

    private long streamOffset;

    private Converter converter = new Converter();

    @Override
    public String version() {
        return new FileSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        realFile = props.get(FileSourceConnector.REAL_FILE_CONFIG);
        rolledFile = props.get(FileSourceConnector.ROLLED_FILE_CONFIG);
        topic = props.get(FileSourceConnector.TOPIC_CONFIG);

        if (topic == null)
            throw new ConnectException("FileSourceTask config missing topic setting");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (stream == null) {
            try {
                stream = new FileInputStream(realFile);
                Map<String, Object> offset =
                    context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, rolledFile));

                if (offset != null) {
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        LOGGER.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        long skipLeft = (Long) lastRecordedOffset;
                        while (skipLeft > 0) {
                            try {
                                long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException e) {
                                LOGGER.error("Error while trying to seek to previous offset in file: ", e);
                                throw new ConnectException(e);
                            }
                        }
                        LOGGER.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
                } else {
                    streamOffset = 0L;
                }
                reader = new BufferedReader(new InputStreamReader(stream));
                LOGGER.debug("Opened {} for reading", realFile);
            } catch (FileNotFoundException e) {
                LOGGER.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait for it to be created");
                synchronized (this) {
                    this.wait(1000);
                }
                return null;
            }
        }
        if (streamOffset >= MAX_FILE_SIZE_BYTES) {
            return Collections.emptyList();
        }

        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
        // is available.
        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                return null;

            List<SourceRecord> records = new ArrayList<>();

            int nread = 0;
            while (readerCopy.ready()) {
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                LOGGER.trace("Read {} bytes from {}", nread, realFile);

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }

                    String line;
                    do {
                        line = extractLine();
                        if (line != null) {
                            LOGGER.trace("Read a line from {}", realFile);
                            Struct record = converter.convertToStruct(line);
                            records.add(new SourceRecord(offsetKey(rolledFile), offsetValue(streamOffset), topic, converter.getSchema(), record));
                        }
                    } while (line != null && stream != null);
                }
            }

            if (nread <= 0)
                synchronized (this) {
                    this.wait(1000);
                }

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;

            streamOffset += newStart;

            return result;
        } else {
            return null;
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    LOGGER.trace("Closed input stream");
                }
            } catch (IOException e) {
                LOGGER.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    public static void main(String[] args) throws InterruptedException {
        FileSourceTask task = new FileSourceTask();
        Map<String, String> props = new HashMap<>();
        props.put(FileSourceConnector.REAL_FILE_CONFIG, "/tmp/apache-test.log");
        props.put(FileSourceConnector.ROLLED_FILE_CONFIG, "/tmp/apache-test-2016-08-30.0.log");
        props.put(FileSourceConnector.TOPIC_CONFIG, "tweets");

        SourceTaskContext sourceTaskContext = () -> new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                return null;
            }
        };

        task.initialize(sourceTaskContext);
        task.start(props);
        task.poll();
    }
}
