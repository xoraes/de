package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by n.dhupia on 8/20/15.
 */
public class FileLoader {
    private static final Logger logger = LoggerFactory.getLogger(FileLoader.class);

    public static Table<String, String, Long> getTable(String target) throws DeException {
        DataFile f = null;
        switch (target) {
            case DeHelper.CATEGORIESBYCOUNTRY:
                f = DataFile.COUNTRY_CATEGORY_FILE;
                break;
            case DeHelper.DEVICESBYCOUNTRY:
                f = DataFile.COUNTRY_DEVICE_FILE;
                break;
            case DeHelper.EVENTSBYCOUNTRY:
                f = DataFile.COUNTRY_EVENT_FILE;
                break;
            case DeHelper.FORMATSBYCOUNTRY:
                f = DataFile.COUNTRY_FORMAT_FILE;
                break;
            case DeHelper.LANGUAGEBYCOUNTRY:
                f = DataFile.COUNTRY_LANG_FILE;
                break;
        }

        Table<String, String, Long> t = HashBasedTable.create();

        long total = 0;
        for (String line : getLines(f.toString())) {
            String[] row = line.split(",");
            total = total + Long.valueOf(row[2]);
            t.put(StringUtils.lowerCase(row[0]), StringUtils.lowerCase(row[1]), Long.valueOf(row[2]));
        }
        t.put("total", "total", total); // set overall total
        // set column key totals
        for (String column : t.columnKeySet()) {
            Long tc = 0L;
            for (Long v : t.column(column).values()) {
                tc = v + tc;
            }
            t.put("total", column, tc);
        }
        // set country totals
        for (String rowKey : t.rowKeySet()) {
            Long tc = 0L;
            for (Long v : t.row(rowKey).values()) {
                tc = v + tc;
            }
            t.put(rowKey, "total", tc);
        }
        //logger.info(t.toString());
        return t;
    }

    private static List<String> getLines(String filename) throws DeException {
        InputStream in = FileLoader.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> lines = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        } finally {
            try {
                reader.close();
                in.close();
            } catch (IOException e) {
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        return lines;
    }

    public enum DataFile {
        COUNTRY_CATEGORY_FILE("country_category.txt"),
        COUNTRY_DEVICE_FILE("country_device.txt"),
        COUNTRY_FORMAT_FILE("country_format.txt"),
        COUNTRY_LANG_FILE("country_lang.txt"),
        COUNTRY_EVENT_FILE("country_event.txt");

        private final String text;

        /**
         * @param text
         */
        DataFile(final String text) {
            this.text = text;
        }

        /* (non-Javadoc)
         * @see java.lang.Enum#toString()
         */
        @Override
        public String toString() {
            return text;
        }
    }
}
