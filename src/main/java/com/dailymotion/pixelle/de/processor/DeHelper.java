package com.dailymotion.pixelle.de.processor;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringProperty;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.util.List;

import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/6/14.
 */
public final class DeHelper {
    public static final DynamicStringProperty organicIndex =
            getInstance().getStringProperty("organicIndex", "organic");
    public static final DynamicStringProperty promotedIndex =
            getInstance().getStringProperty("promotedIndex", "promoted");
    public static final DynamicStringProperty channelIndex =
            getInstance().getStringProperty("channelIndex", "channel");
    public static final DynamicStringProperty adunitsType =
            getInstance().getStringProperty("adunitsType", "adunits");
    public static final DynamicStringProperty videosType =
            getInstance().getStringProperty("videosType", "videos");
    public static final DynamicStringProperty clusterName =
            getInstance().getStringProperty("clusterName", "pixelle");
    public static final DynamicStringProperty nodeName =
            getInstance().getStringProperty("nodeName", "pixellenode");
    public static final DynamicStringProperty domain =
            getInstance().getStringProperty("domain", "pxlad.io");
    public static final DynamicStringProperty dataDirectory =
            getInstance().getStringProperty("datadir", "/data/es");
    public static final DynamicStringProperty widgetPattern =
            getInstance().getStringProperty("widget.pattern", "oop");
    public static final DynamicIntProperty retryOnConflictAdUnits =
            getInstance().getIntProperty("adunits.retryOnConflict", 5);
    public static final DynamicIntProperty retryOnConflictVideos =
            getInstance().getIntProperty("videos.retryOnConflict", 5);
    public static final DynamicIntProperty maxImpressions =
            getInstance().getIntProperty("impressions.max", 3);

    public static final DynamicIntProperty dePort =
            getInstance().getIntProperty("port", 8080);
    public static final String EVENTSBYCOUNTRY = "COUNTRY_EVENTS";
    public static final String LANGUAGEBYCOUNTRY = "COUNTRY_LANG";
    public static final String CATEGORIESBYCOUNTRY = "COUNTRY_CATEGORY";
    public static final String DEVICESBYCOUNTRY = "COUNTRY_DEVICE";
    public static final String FORMATSBYCOUNTRY = "COUNTRY_FORMAT";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static Logger logger = getLogger(DeHelper.class);

    private DeHelper() {
    }

    public static Boolean isEmptyList(List<?> cList) {
        return cList == null || cList.isEmpty();
    }

    public static DateTime currentUTCTime() {
        return now().withZone(UTC);
    }

    public static String currentUTCTimeString() {
        return timeToISO8601String(currentUTCTime());
    }

    public static String timeToISO8601String(DateTime dt) {
        return dt.withZone(UTC).toString("yyyy-MM-dd'T'HH:mm:ssZZ");
    }

    public static String getDateTimeFormatString() {
        return DATETIME_FORMAT;
    }

    public static List<String> toLowerCase(List<String> list) {
        if (!isEmptyList(list)) {
            for (int i = 0; i < list.size(); i++) {
                list.set(i, list.get(i).toLowerCase());
            }
        }
        return list;
    }

    public enum FORMAT {
        INWIDGET("in-widget"),
        INRELATED("in-related"),
        INSEARCH("in-search"),
        INFEED("in-feed");


        private final String text;

        /**
         * @param text
         */
        FORMAT(final String text) {
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
