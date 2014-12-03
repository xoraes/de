package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by n.dhupia on 11/6/14.
 */
public class DeHelper {
    private static Logger logger = LoggerFactory.getLogger(DeHelper.class);
    private static DynamicStringProperty index =
            DynamicPropertyFactory.getInstance().getStringProperty("index", "");
    private static DynamicStringProperty adType =
            DynamicPropertyFactory.getInstance().getStringProperty("adUnitsType", "");
    private static DynamicStringProperty ovType =
            DynamicPropertyFactory.getInstance().getStringProperty("ovType", "");
    private static DynamicStringProperty clusterName =
            DynamicPropertyFactory.getInstance().getStringProperty("clusterName", "");
    private static DynamicStringProperty nodeName =
            DynamicPropertyFactory.getInstance().getStringProperty("nodeName", "");
    private static DynamicStringProperty dataDirectory =
            DynamicPropertyFactory.getInstance().getStringProperty("datadir", "/data/es");
    private static DynamicIntProperty dePort =
            DynamicPropertyFactory.getInstance().getIntProperty("port", 8080);
    private static DynamicStringProperty privateIp =
            DynamicPropertyFactory.getInstance().getStringProperty("privateIP", "");
    private static DynamicStringProperty rmiPort =
            DynamicPropertyFactory.getInstance().getStringProperty("rmiPort", "8005");

    public static String getRmiPort() {
        return rmiPort.get();
    }
    public static String getIpAddr() {
        return privateIp.get();
    }

    public static String getCluster() {
        return clusterName.get();
    }

    public static String getNode() {
        return nodeName.get();
    }

    public static String getIndex() {
        return index.get();
    }

    public static String getAdUnitsType() {

        return adType.get();
    }

    public static String getOrganicVideoType() {

        return ovType.get();
    }

    public static String getDataDir() {

        return dataDirectory.get();
    }


    public static List<AdUnitResponse> removeDuplicateCampaigns(int positions, List<AdUnitResponse> units) {
        int count = 1;
        Map<String, Integer> m = new HashMap<String, Integer>();
        List<AdUnitResponse> uniqueAds = new ArrayList<AdUnitResponse>();

        for (AdUnitResponse unit : units) {
            if (count > positions) {
                break;
            }
            if (!m.containsKey(unit.getCampaignId())) {
                m.put(unit.getCampaignId(), 1);
                uniqueAds.add(unit);
                count++;
            }
        }
        return uniqueAds;
    }

    public static List<String> stringListToLowerCase(List<String> listStr) {
        for (int i = 0; i < listStr.size(); i++) {
            listStr.set(i, listStr.get(i).toLowerCase());
        }
        return listStr;
    }

    public static Boolean isHourSet(int hour, int mask) {
        return (mask & (1 << hour)) > 0;
    }

    public static List<String> convertSchedulesToTimeTable(Integer[] schedules) {
        String day;
        Boolean hourSet;
        List<String> timeTable = null;
        if (schedules != null && schedules.length > 0) {
            timeTable = new ArrayList<String>();
            for (int i = 0; i < 7; i++) {
                LocalDate date = new LocalDate();
                if (i == 0) {
                    date = date.withDayOfWeek(7);
                } else {
                    date = date.withDayOfWeek(i);
                }
                day = date.dayOfWeek().getAsText().toLowerCase();
                for (int j = 0; j < 24; j++) {
                    hourSet = isHourSet(j, schedules[i]);
                    timeTable.add(day + ":" + j + ":" + hourSet.toString());
                }
            }
        }
        return timeTable;
    }

    public static Boolean isEmptyArray(List<? extends Object> cList) {
        if (cList == null || cList.size() == 0) {
            return true;
        }
        return false;
    }

    public static DateTime currentUTCTime() {
        return DateTime.now().withZone(DateTimeZone.UTC);
    }

    public static String currentUTCTimeString() {
        return timeToISO8601String(currentUTCTime());
    }

    public static String timeToISO8601String(DateTime dt) {
        return dt.toString("yyyy-MM-dd'T'HH:mm:ssZ");
    }

    public static int getPort() {

        return dePort.get();
    }
}
