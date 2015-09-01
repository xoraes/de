package com.dailymotion.pixelle.common.services;

import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.google.common.collect.Table;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static com.dailymotion.pixelle.common.services.FileLoader.DataFile.COUNTRY_CATEGORY_FILE;
import static com.dailymotion.pixelle.common.services.FileLoader.DataFile.COUNTRY_DEVICE_FILE;
import static com.dailymotion.pixelle.common.services.FileLoader.DataFile.COUNTRY_EVENT_FILE;
import static com.dailymotion.pixelle.common.services.FileLoader.DataFile.COUNTRY_FORMAT_FILE;
import static com.dailymotion.pixelle.common.services.FileLoader.DataFile.COUNTRY_LANG_FILE;
import static com.dailymotion.pixelle.de.processor.DeHelper.CATEGORIESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.DEVICESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.EVENTSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMATSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.LANGUAGEBYCOUNTRY;
import static com.google.common.collect.HashBasedTable.create;
import static java.lang.Long.valueOf;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 8/20/15.
 */
public class FileLoader {
    private static final Logger LOGGER = getLogger(FileLoader.class);

    public static Table<String, String, Long> getTable(String target) throws ForecastException {
        DataFile f = null;
        switch (target) {
            case CATEGORIESBYCOUNTRY:
                f = COUNTRY_CATEGORY_FILE;
                break;
            case DEVICESBYCOUNTRY:
                f = COUNTRY_DEVICE_FILE;
                break;
            case EVENTSBYCOUNTRY:
                f = COUNTRY_EVENT_FILE;
                break;
            case FORMATSBYCOUNTRY:
                f = COUNTRY_FORMAT_FILE;
                break;
            case LANGUAGEBYCOUNTRY:
                f = COUNTRY_LANG_FILE;
                break;
        }

        Table<String, String, Long> t = create();

        long total = 0;
        for (String line : getLines(f.toString())) {
            String[] row = line.split(",");
            total = total + valueOf(row[2]);
            t.put(lowerCase(row[0]), lowerCase(row[1]), valueOf(row[2]));
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

    private static List<String> getLines(String filename) throws ForecastException {
        InputStream in = FileLoader.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> lines = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        } finally {
            try {
                reader.close();
                in.close();
            } catch (IOException e) {
                throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
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
