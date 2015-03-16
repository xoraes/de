package com.dailymotion.pixelle.deserver.processor;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 11/6/14.
 */
public final class DeHelper {
    private static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final DynamicStringProperty organicIndex =
            DynamicPropertyFactory.getInstance().getStringProperty("organicIndex", "organic");
    public static final DynamicStringProperty promotedIndex =
            DynamicPropertyFactory.getInstance().getStringProperty("promotedIndex", "promoted");
    public static final DynamicStringProperty channelIndex =
            DynamicPropertyFactory.getInstance().getStringProperty("channelIndex", "channel");
    public static final DynamicStringProperty adunitsType =
            DynamicPropertyFactory.getInstance().getStringProperty("adunitsType", "adunits");
    public static final DynamicStringProperty videosType =
            DynamicPropertyFactory.getInstance().getStringProperty("videosType", "videos");
    public static final DynamicStringProperty clusterName =
            DynamicPropertyFactory.getInstance().getStringProperty("clusterName", "pixelle");
    public static final DynamicStringProperty nodeName =
            DynamicPropertyFactory.getInstance().getStringProperty("nodeName", "pixellenode");
    public static final DynamicStringProperty domain =
            DynamicPropertyFactory.getInstance().getStringProperty("domain", "pxlad.io");
    public static final DynamicStringProperty dataDirectory =
            DynamicPropertyFactory.getInstance().getStringProperty("datadir", "/data/es");
    private static final DynamicIntProperty dePort =
            DynamicPropertyFactory.getInstance().getIntProperty("port", 8080);
    public static final DynamicStringProperty widgetPattern =
            DynamicPropertyFactory.getInstance().getStringProperty("widget.pattern", "oop");
    public static final DynamicIntProperty retryOnConflictAdUnits =
            DynamicPropertyFactory.getInstance().getIntProperty("adunits.retryOnConflict", 5);
    public static final DynamicIntProperty retryOnConflictVideos =
            DynamicPropertyFactory.getInstance().getIntProperty("videos.retryOnConflict", 5);

    private static Logger logger = LoggerFactory.getLogger(DeHelper.class);


    private DeHelper() {
    }

    public static List<String> stringListToLowerCase(List<String> listStr) {
        if (isEmptyList(listStr)) {
            return null;
        }
        for (int i = 0; i < listStr.size(); i++) {
            String str = StringUtils.lowerCase(listStr.get(i));
            listStr.set(i, str);
        }
        return listStr;
    }

    public static Boolean isEmptyList(List<?> cList) {
        return cList == null || cList.isEmpty();
    }

    public static DateTime currentUTCTime() {
        return DateTime.now().withZone(DateTimeZone.UTC);
    }

    public static String currentUTCTimeString() {
        return timeToISO8601String(currentUTCTime());
    }

    public static String timeToISO8601String(DateTime dt) {
        return dt.withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ssZZ");
    }

    public static int getPort() {

        return dePort.get();
    }

    public static String getDateTimeFormatString() {
        return DATETIME_FORMAT;
    }

    public static List<String> toLowerCase(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            list.set(i, list.get(i).toLowerCase());
        }
        return list;
    }
}
