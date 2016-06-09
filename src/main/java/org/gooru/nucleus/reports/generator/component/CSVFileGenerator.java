package org.gooru.nucleus.reports.generator.component;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.gooru.nucleus.reports.infra.component.UtilityManager;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;

public class CSVFileGenerator {

    private static final String DEFAULT_FILE_NAME = "insights";
    private static final UtilityManager um = UtilityManager.getInstance();
    private static final Pattern FIELDS_TO_TIME_FORMAT =
        Pattern.compile("time_spent|timeSpent|totalTimespent|avgTimespent|timespent|totalTimeSpentInMs|.*Timespent.*");

    public static File generateCSVReport(boolean isNewFile, String folderName, String fileName,
        List<Map<String, Object>> resultSet) throws IOException {

        boolean headerColumns = false;
        if (folderName != null) {
            File dirs = new File(UtilityManager.getFileSaveRealPath() + folderName);
            dirs.mkdirs();
        }
        File csvfile = new File(setFilePath(fileName));
        PrintStream stream = generatePrintStream(isNewFile, csvfile);
        for (Map<String, Object> map : resultSet) {
            writeToStream(map, stream, headerColumns);
            headerColumns = true;
        }
        writeToFile(stream);
        return csvfile;
    }

    public File generateCSVReport(boolean isNewFile, String fileName, Map<String, Object> resultSet)
        throws IOException {

        boolean headerColumns = false;
        File csvfile = new File(setFilePath(fileName));
        PrintStream stream = generatePrintStream(isNewFile, csvfile);
        writeToStream(resultSet, stream, headerColumns);
        writeToFile(stream);
        return csvfile;
    }

    public static void includeEmptyLine(boolean isNewFile, String fileName, int lineCount)
        throws FileNotFoundException {

        File csvfile = new File(setFilePath(fileName));
        PrintStream stream = generatePrintStream(isNewFile, csvfile);
        for (int i = 0; i < lineCount; i++) {
            stream.println(ConfigConstants.STRING_EMPTY);
        }
        writeToFile(stream);
    }

    private static Object appendDQ(Object key) {
        return ConfigConstants.DOUBLE_QUOTES + key + ConfigConstants.DOUBLE_QUOTES;
    }

    private static void writeToStream(Map<String, Object> map, PrintStream stream, boolean headerColumns) {
        if (!headerColumns) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                stream.print(appendDQ(entry.getKey()) + ConfigConstants.COMMA);
            }
            // print new line
            stream.println("");
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (FIELDS_TO_TIME_FORMAT.matcher(entry.getKey()).matches()) {
                value = convertMillisecondsToTime(((Number) value).longValue());
            }
            stream.print(appendDQ(value) + ConfigConstants.COMMA);
        }
        // print new line
        stream.println(ConfigConstants.STRING_EMPTY);
    }

    private static PrintStream generatePrintStream(boolean isNewFile, File file) throws FileNotFoundException {
        // TODO: AM: Who is responsible for closing this stream
        PrintStream stream;
        if (isNewFile) {
            stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file, false)));
        } else {
            stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file, true)));
        }
        return stream;
    }

    private static void writeToFile(PrintStream stream) {
        stream.flush();
        stream.close();
    }

    private static String setFilePath(String file) {

        String fileName = UtilityManager.getFileSaveRealPath();
        if (file != null && (!file.isEmpty())) {
            fileName += file;
        } else {
            fileName += DEFAULT_FILE_NAME;
        }
        return fileName + ConfigConstants.CSV_EXT;
    }

    private static String convertMillisecondsToTime(long millis) {
        return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
            TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
            TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1));
    }
}
