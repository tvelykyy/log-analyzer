package com.tvelykyy.filekafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DirectoryReader {
    private static final String DATE_FORMAT = "%d\\{(.*)\\}";

//    private final String BASE_DIR = "/tmp/";
//    private final String CURRENT_FILE = BASE_DIR + "tmp/apache.log";
//    private final String ROLLED_FILES_PATTERN = BASE_DIR + "tmp/apache-%d{yyyy-MM-dd}.%i.log";

    private final String baseDir;
    private final String currentFile;
    private final String rolledFilesPattern;

    public DirectoryReader(String baseDir, String currentFile, String rolledFilesPattern) {
        this.baseDir = baseDir;
        this.currentFile = baseDir + currentFile;
        this.rolledFilesPattern = baseDir + rolledFilesPattern;
    }

    public Map<String, String> getMappedFiles()  {
        String date = getDate();
        //We need to convert rolledFilesPattern to pattern with current date
        String rolledPattern = rolledFilesPattern.replaceAll("/", "\\\\/").replaceAll(DATE_FORMAT, date).replaceAll("%i", "\\\\d*");

        Pattern pattern = Pattern.compile(rolledPattern);

        //get rolled files
        List<String> rolled = new LinkedList<>();
        try (Stream<Path> stream = Files.list(Paths.get(baseDir))) {
             rolled = stream
                    .map(String::valueOf)
                    .filter(path -> pattern.matcher(path).matches())
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, String> files = new HashMap<>(rolled.size() + 1);
        for (String rolledFile : rolled) {
            files.put(rolledFile, rolledFile);
        }
        //Getting rolled filename for current file to store it
        files.put(currentFile, nextFile(date, rolled.size()));

        return files;
    }

    private String nextFile(String date, int count) {
        return rolledFilesPattern.replaceAll(DATE_FORMAT, date).replaceAll("%i", String.valueOf(count));
    }

    private String getDate() {
        Pattern datePattern = Pattern.compile(DATE_FORMAT);
        Matcher matcher = datePattern.matcher(rolledFilesPattern);
        String dateFormat = null;
        if (matcher.find()) {
            dateFormat = matcher.group(1);
        }

        SimpleDateFormat simple = new SimpleDateFormat(dateFormat);

        return simple.format(new Date());
    }

    public static void main(String[] args) throws IOException {
//        getMappedFiles();
        DirectoryReader directoryReader = new DirectoryReader("/tmp/", "apache.log", "apache-%d{yyyy-MM-dd}.%i.log");
        directoryReader.getMappedFiles();
    }
}
