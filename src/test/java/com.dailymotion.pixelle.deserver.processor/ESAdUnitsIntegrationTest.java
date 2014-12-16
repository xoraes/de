package com.dailymotion.pixelle.deserver.processor;


import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.hystrix.AdInsertCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.deserver.providers.ESTestNodeClientProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ESAdUnitsIntegrationTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static Injector injector;
    private static DEProcessor es;

    @BeforeClass
    public static void setUp() throws Exception {
        ConfigurationManager.loadCascadedPropertiesFromResources("de");
        System.out.println("Running Setup");

        Injector injector = Guice.createInjector(new AbstractModule() {

            @Override
            protected void configure() {
                bind(Client.class).toProvider(ESTestNodeClientProvider.class).asEagerSingleton();
                bind(AdUnitProcessor.class).asEagerSingleton();
                bind(VideoProcessor.class).asEagerSingleton();
                bind(DEProcessor.class).to(DEProcessorImpl.class).asEagerSingleton();
            }
        });
        es = injector.getInstance(DEProcessor.class);

    }

    public static void setProcessor(DEProcessor deProcessor) {
        es = deProcessor;

    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("Deleting the index");
        es.deleteIndex();
        injector = null;
    }

    public static Map<String, Object> createAdUnitDataMap(String id, String cid) {
        String timeNow = DeHelper.currentUTCTimeString();
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("_id", id);
        m.put("ad", id);
        m.put("tactic", "1");
        m.put("account", "1");
        m.put("campaign", cid);
        m.put("_updated", timeNow);
        m.put("_created", timeNow);
        m.put("categories", new ArrayList<String>(Arrays.asList("cat1", "cat2")));
        m.put("devices", new ArrayList<String>(Arrays.asList("dev1", "dev2")));
        m.put("formats", new ArrayList<String>(Arrays.asList("fmt1", "fmt2")));
        m.put("locations", new ArrayList<String>(Arrays.asList("us", "fr")));
        m.put("languages", new ArrayList<String>(Arrays.asList("en", "fr")));
        m.put("excluded_locations", new ArrayList<String>(Arrays.asList("nn", "mm")));
        m.put("excluded_categories", new ArrayList<String>(Arrays.asList("tac", "tacc")));
        m.put("schedules", new ArrayList<Integer>(Arrays.asList(16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215)));
        m.put("goal_period", "total");
        m.put("title", "title");
        m.put("description", "description");
        m.put("channel", "channel");
        m.put("channel_url", "channel_url");
        m.put("channel_name", "channel_name");
        m.put("channel_id", "channel_id");
        m.put("start_date", "2014-11-01T00:00:00Z");
        m.put("end_date", "2114-11-01T00:00:00Z");
        m.put("resizable_thumbnail_url", "resizable_thumbnail_url");
        m.put("thumbnail_url", "thumbnail_url");
        m.put("IGNOREME", "IGNOREME"); //Unknown json should be ignored
        m.put("video_id", "video_id");
        m.put("status", "active");
        m.put("duration", 123);
        m.put("goal_views", 123);
        m.put("cpc", 10);
        m.put("goal_reached", false);
        return m;
    }

    public static void loadAdUnitMaps(DEProcessor deProcessor, Map<String, Object>... map) throws Exception {
        es = deProcessor;
        loadAdUnitMaps(map);
    }

    public static void loadAdUnitMaps(Map<String, Object>... map) throws Exception {
        String json;
        AdUnit unit;
        for (Map m : map) {
            try {
                //serialize to json
                json = mapper.writeValueAsString(m);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                throw new Exception("failed parsing json", e);
            }
            if (json != null) {
                //deserialize to adunit
                unit = mapper.readValue(json, AdUnit.class);
                Assert.assertTrue(new AdInsertCommand(es, unit).execute());
            }

        }
        Thread.sleep(2000);
    }

    public static void deleteAdUnitsByIds(DEProcessor deProcessor, String... ids) throws Exception {
        es = deProcessor;
        deleteAdUnitsByIds(ids);
    }

    public static void deleteAdUnitsByIds(String... ids) throws Exception {
        for (String id : ids) {
            Assert.assertTrue(es.deleteById(DeHelper.getIndex(), DeHelper.getAdUnitsType(), id));
        }
    }

    @Test
    public void testCheckAllFields() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        ItemsResponse i = new QueryCommand(es, sq, 1, "promoted").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        AdUnitResponse adunit = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertTrue(adunit.getAd().equals("1"));
        Assert.assertTrue(adunit.getChannel().equals("channel"));
        Assert.assertTrue(adunit.getChannelId().equals("channel_id"));
        Assert.assertTrue(adunit.getChannelName().equals("channel_name"));
        Assert.assertTrue(adunit.getChannelUrl().equals("channel_url"));
        Assert.assertTrue(adunit.getAccountId().equals("1"));
        Assert.assertTrue(adunit.getVideoId().equals("video_id"));
        Assert.assertTrue(adunit.getCampaignId().equals("1"));
        Assert.assertTrue(adunit.getContentType().equals("promoted"));
        Assert.assertTrue(adunit.getCpc() == 10);
        Assert.assertTrue(adunit.getDescription().equals("description"));
        Assert.assertTrue(adunit.getTitle().equals("title"));
        Assert.assertTrue(adunit.getDuration() == 123);
        Assert.assertTrue(adunit.getThumbnailUrl().equals("thumbnail_url"));
        Assert.assertTrue(adunit.getResizableThumbnailUrl().equals("resizable_thumbnail_url"));
        Assert.assertTrue(adunit.getTacticId().equals("1"));

        deleteAdUnitsByIds("1");
    }

    @Test
    public void testMultipleInsertAdUnit() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");
        loadAdUnitMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("EN")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("US")));

        ItemsResponse i = new QueryCommand(es, sq, 3, "promoted").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testGetLastUpdated() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");
        Map m4 = createAdUnitDataMap("4", "4");
        Map m5 = createAdUnitDataMap("5", "5");
        Map m6 = createAdUnitDataMap("6", "6");

        m1.put("_updated", "2014-01-01T00:00:01Z");
        m2.put("_updated", "2014-01-01T00:00:14Z");
        m3.put("_updated", "2014-01-01T00:00:19Z");
        m4.put("_updated", "2014-01-01T00:00:11Z");
        m5.put("_updated", "2014-01-01T00:00:02Z");
        m6.put("_updated", "2014-01-01T00:01:00Z");

        Thread.sleep(2000);
        String datetime = es.getLastUpdatedTimeStamp(DeHelper.getAdUnitsType());
        Assert.assertNotNull(datetime);
        System.out.println("DATETIME ====>:" + datetime);
        loadAdUnitMaps(m1, m2, m3, m4, m5, m6);

        datetime = es.getLastUpdatedTimeStamp(DeHelper.getAdUnitsType());
        System.out.println("DATETIME ====>:" + datetime);
        Assert.assertNotNull(datetime);
        Assert.assertEquals("2014-01-01T00:01:00Z", datetime);
        deleteAdUnitsByIds("1", "2", "3", "4", "5", "6");
    }

    @Test
    public void testGetAdUnitsByCampaign() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "1");
        Map m3 = createAdUnitDataMap("3", "1");

        loadAdUnitMaps(m1, m2, m3);
        Assert.assertEquals(3, es.getAdUnitsByCampaign("1").size());
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testRemoveDupCampaignFromAdUnit() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "1");
        loadAdUnitMaps(m1, m2);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        deleteAdUnitsByIds("1", "2");
    }

    @Test
    public void testScheduleTargeting() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("schedules", new ArrayList<Integer>(Arrays.asList(0, 0, 0, 0, 0, 16777215, 0)));
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-11-21T01:00:00Z"); //2014-11-21 is a friday
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testTimeTargeting() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("start_date", "2014-11-01T00:00:00Z");
        m1.put("end_date", "2114-11-01T00:00:00Z");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-10-31T23:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testTimeTargetingNegative() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("start_date", "2014-11-01T00:00:00Z");
        m1.put("end_date", "2114-11-01T00:00:00Z");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-10-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testGoalReached() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("goal_reached", true);
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testLanguageTargetingNegative() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");
        m1.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m2.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m3.put("languages", new ArrayList<String>(Arrays.asList("fr")));
        loadAdUnitMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 2);
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testStatusInActive() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("status", "inactive");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1");
    }
}

