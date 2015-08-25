package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by n.dhupia on 8/20/15.
 */
public class FileLoader {
    private static final Logger logger = LoggerFactory.getLogger(FileLoader.class);

    public static Table<String, String, Long> getTable(String target) {
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
            Long tc = 0l;
            for (Long v : t.column(column).values()) {
                tc = v + tc;
            }
            t.put("total", column, tc);
        }
        // set country totals
        for (String rowKey : t.rowKeySet()) {
            Long tc = 0l;
            for (Long v : t.row(rowKey).values()) {
                tc = v + tc;
            }
            t.put(rowKey, "total", tc);
        }
        return t;
    }

    private static List<String> getLines(String filepath) {
        String filePath = FileLoader.class.getClassLoader().getResource(filepath).getFile();
        logger.info(filePath);
        List<String> lines = null;
        try {
            lines = Files.readLines(new File(filePath), Charsets.UTF_8);
        } catch (IOException e) {
            logger.error(e.getMessage());
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
