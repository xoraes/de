package com.dailymotion.pixelle.de;

import com.dailymotion.pixelle.de.model.AdUnit;
import com.dailymotion.pixelle.de.model.AdUnitResponse;
import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.dailymotion.pixelle.de.processor.ChannelProcessor;
import com.dailymotion.pixelle.de.processor.DEProcessor;
import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.DeHelper;
import com.dailymotion.pixelle.de.processor.VideoProcessor;
import com.dailymotion.pixelle.de.processor.hystrix.AdInsertCommand;
import com.dailymotion.pixelle.de.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.de.providers.ESTestNodeClientProvider;
import com.dailymotion.pixelle.forecast.processor.Forecaster;
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

    @BeforeClass
    public static void setUp() throws Exception {
        ConfigurationManager.loadCascadedPropertiesFromResources("application");
        System.out.println("Running Setup");
        Injector injector = Guice.createInjector(new AbstractModule() {

            @Override
            protected void configure() {
                bind(Client.class).toProvider(ESTestNodeClientProvider.class).asEagerSingleton();
                bind(AdUnitProcessor.class).asEagerSingleton();
                bind(Forecaster.class).asEagerSingleton();
                bind(VideoProcessor.class).asEagerSingleton();
                bind(ChannelProcessor.class).asEagerSingleton();
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("Deleting all known indices");
        DEProcessor.deleteIndex(DeHelper.organicIndex.get());
        DEProcessor.deleteIndex(DeHelper.promotedIndex.get());

    }

    public static Map<String, Object> createAdUnitDataMap(String id, String cid) {
        String timeNow = DeHelper.currentUTCTimeString();
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("_id", id);
        m.put("clicks", 101);
        m.put("impressions", 1000);
        m.put("ad", id);
        m.put("tactic", "1");
        m.put("account", "1");
        m.put("campaign", cid);
        m.put("_updated", "2014-11-01T00:00:00Z");
        m.put("_created", "2014-11-01T00:00:00Z");
        m.put("categories", new ArrayList<String>(Arrays.asList("cat1", "cat2")));
        m.put("devices", new ArrayList<String>(Arrays.asList("dev1", "dev2")));
        m.put("formats", new ArrayList<String>(Arrays.asList(DeHelper.FORMAT.INWIDGET.toString(), DeHelper.FORMAT.INFEED.toString())));
        m.put("locations", new ArrayList<String>(Arrays.asList("us", "fr")));
        m.put("languages", new ArrayList<String>(Arrays.asList("en", "fr")));
        m.put("excluded_locations", new ArrayList<String>(Arrays.asList("nn", "mm")));
        m.put("excluded_categories", new ArrayList<String>(Arrays.asList("tac", "tacc")));
        m.put("schedules", new ArrayList<Integer>(Arrays.asList(16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215)));
        m.put("goal_period", "total");
        m.put("title", "title");
        m.put("description", "description");
        m.put("channel", "channel");
        m.put("channel_name", "channel_name");
        m.put("channel_id", "channel_id");
        m.put("start_date", "2014-11-01T00:00:00Z");
        m.put("end_date", "2114-11-01T00:00:00Z");
        m.put("resizable_thumbnail_url", "resizable_thumbnail_url");
        m.put("custom_video_url", "custom_video_url");
        m.put("IGNOREME", "IGNOREME"); //Unknown json should be ignored
        m.put("video_id", "video_id");
        m.put("duration", 123);
        m.put("cpc", 10);
        m.put("internal_cpv", 10);
        m.put("currency", "USD");
        m.put("cpv", 10);
        return m;
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
                new AdInsertCommand(unit).execute();
            }

        }
        Thread.sleep(2000);
    }

    public static void deleteAdUnitsByIds(String... ids) throws DeException {
        for (String id : ids) {
            Assert.assertTrue(DEProcessor.deleteById(DeHelper.promotedIndex.get(), DeHelper.adunitsType.get(), id));
        }
    }

    @Test
    public void testCategoryTargetingNegative() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(Arrays.asList("cat5")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        ItemsResponse i = new QueryCommand(sq, 1, "promoted").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testCheckAllFields() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        ItemsResponse i = new QueryCommand(sq, 1, "promoted").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        AdUnitResponse adunit = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertTrue(adunit.getAd().equals("1"));
        Assert.assertTrue(adunit.getChannel().equals("channel"));
        Assert.assertTrue(adunit.getChannelId().equals("channel_id"));
        Assert.assertTrue(adunit.getChannelName().equals("channel_name"));
        Assert.assertTrue(adunit.getAccountId().equals("1"));
        Assert.assertTrue(adunit.getVideoId().equals("video_id"));
        Assert.assertTrue(adunit.getCampaignId().equals("1"));
        Assert.assertTrue(adunit.getContentType().equals("promoted"));
        Assert.assertTrue(adunit.getCpc() == 10);
        Assert.assertTrue(adunit.getDescription().equals("description"));
        Assert.assertTrue(adunit.getTitle().equals("title"));
        Assert.assertTrue(adunit.getDuration() == 123);
        Assert.assertTrue(adunit.getResizableThumbnailUrl().equals("resizable_thumbnail_url"));
        Assert.assertTrue(adunit.getCustomVideoUrl().equals("custom_video_url"));
        Assert.assertTrue(adunit.getTacticId().equals("1"));
        Assert.assertTrue(adunit.getCpv() == 10);
        Assert.assertTrue(adunit.getInternalCpv() == 10);
        Assert.assertTrue(adunit.getCurrency().equals("USD"));

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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("EN")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("US")));
        sq.setDebugEnabled(true);

        ItemsResponse i = new QueryCommand(sq, 3, "promoted").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testGetAdUnitsByCampaign() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "1");
        Map m3 = createAdUnitDataMap("3", "1");

        loadAdUnitMaps(m1, m2, m3);
        Assert.assertEquals(3, DEProcessor.getAdUnitsByCampaign("1").size());
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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));

        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);

        sq.setTime("2015-03-16T01:00:00Z");//Monday
        i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertTrue(i.getResponse().size() == 0);

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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testGoalReached() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("views", 1);
        m1.put("goal_views", 1);
        Map m2 = createAdUnitDataMap("2", "2");
        m2.put("views", 2);
        m2.put("goal_views", 2);
        loadAdUnitMaps(m1, m2);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteAdUnitsByIds("1", "2");
    }

    @Test
    public void testGoalReachedTest2() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        m1.put("goal_views", 1);
        Map m2 = createAdUnitDataMap("2", "2");
        m2.put("views", 1);
        m2.put("goal_views", 2);
        Map m3 = createAdUnitDataMap("3", "3");
        m3.put("views", 3);
        m3.put("goal_views", 2);

        Map m4 = createAdUnitDataMap("4", "4");
        m4.put("views", 3);

        Map m5 = createAdUnitDataMap("5", "5");


        loadAdUnitMaps(m1, m2, m3, m4, m5);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 4);
        deleteAdUnitsByIds("1", "2", "3", "4", "5");
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
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 2);
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testCpvBoosting() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");
        m1.put("internal_cpv", 1);
        m2.put("internal_cpv", 2);
        m3.put("internal_cpv", 3);
        loadAdUnitMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(3, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        AdUnitResponse r2 = (AdUnitResponse) i.getResponse().get(1);
        AdUnitResponse r3 = (AdUnitResponse) i.getResponse().get(2);
        Assert.assertEquals("3", r1.getCampaignId());
        Assert.assertEquals("2", r2.getCampaignId());
        Assert.assertEquals("1", r3.getCampaignId());
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testCtrBoosting() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");

        m1.put("clicks", 101.0);
        m1.put("impressions", 1000.0);
        m2.put("clicks", 200.0);
        m2.put("impressions", 1000.0);
        m3.put("clicks", 300.0);
        m3.put("impressions", 1000.0);
        loadAdUnitMaps(m1, m2, m3);

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(3, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        AdUnitResponse r2 = (AdUnitResponse) i.getResponse().get(1);
        AdUnitResponse r3 = (AdUnitResponse) i.getResponse().get(2);
        Assert.assertEquals("3", r1.getCampaignId());
        Assert.assertEquals("2", r2.getCampaignId());
        Assert.assertEquals("1", r3.getCampaignId());
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testExploration() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");

        m1.put("_created", DeHelper.currentUTCTimeString());
        m2.put("_created", "2014-10-31T00:00:00Z");
        loadAdUnitMaps(m1, m2);

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2015-10-29T01:00:00Z");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 2, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(2, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        AdUnitResponse r2 = (AdUnitResponse) i.getResponse().get(1);
        Assert.assertEquals("1", r1.getCampaignId());
        Assert.assertEquals("2", r2.getCampaignId());
        deleteAdUnitsByIds("1", "2");
    }


    @Test
    public void testCpvAndCtrCombinedBoosting() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        Map m3 = createAdUnitDataMap("3", "3");
        Map m4 = createAdUnitDataMap("4", "4");
        Map m5 = createAdUnitDataMap("5", "5");
        Map m6 = createAdUnitDataMap("6", "6");
        Map m7 = createAdUnitDataMap("7", "7");
        Map m8 = createAdUnitDataMap("8", "8");
        Map m9 = createAdUnitDataMap("9", "9");

        m1.put("internal_cpv", 20);
        m2.put("internal_cpv", 10);
        m3.put("internal_cpv", 10);
        m4.put("internal_cpv", 4);
        m5.put("internal_cpv", 5);
        m6.put("internal_cpv", 13);
        m7.put("internal_cpv", 13);
        m8.put("internal_cpv", 1);
        m9.put("internal_cpv", 13);

        m1.put("clicks", 140.0);
        m1.put("impressions", 10000.0);

        m2.put("clicks", 158.0);
        m2.put("impressions", 10000.0);

        m3.put("clicks", 148.0);
        m3.put("impressions", 10000.0);


        m4.put("clicks", 147.0);
        m4.put("impressions", 10000.0);

        m5.put("clicks", 158.0);
        m5.put("impressions", 10000.0);

        m6.put("clicks", 147.0);
        m6.put("impressions", 10000.0);

        m7.put("clicks", 141.0);
        m7.put("impressions", 10000.0);

        m8.put("clicks", 1005.0);
        m8.put("impressions", 10000.0);

        m9.put("clicks", 155.0);
        m9.put("impressions", 10000.0);


        loadAdUnitMaps(m1, m2, m3, m4, m5, m6, m7, m8, m9);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(3, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        AdUnitResponse r2 = (AdUnitResponse) i.getResponse().get(1);
        AdUnitResponse r3 = (AdUnitResponse) i.getResponse().get(2);
        Assert.assertEquals("1", r1.getCampaignId());
        Assert.assertEquals("9", r2.getCampaignId());
        Assert.assertEquals("6", r3.getCampaignId());
        deleteAdUnitsByIds("1", "2", "3", "4", "5", "6", "7", "8", "9");
    }


    @Test
    public void testImpressionHistory() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        Map m2 = createAdUnitDataMap("2", "2");
        m1.put("video_id", "1");
        m2.put("video_id", "2");
        loadAdUnitMaps(m1, m2);

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);


        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertEquals("1", r1.getCampaignId());

        Map<String, Integer> m = new HashMap<>();
        m.put("1", 11);
        m.put("2", 1);
        sq.setImpressionHistory(m);

        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 1, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        r1 = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertEquals("2", r1.getCampaignId());

        deleteAdUnitsByIds("1", "2");
    }

    @Test
    public void testImpressionHistoryFallback() throws Exception {
        Map m1 = createAdUnitDataMap("1", "1");
        //   Map m2 = createAdUnitDataMap("2", "2");
        m1.put("video_id", "1");
        // m2.put("video_id", "2");
        loadAdUnitMaps(m1);

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(DeHelper.FORMAT.INFEED.toString());
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setDebugEnabled(true);


        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        AdUnitResponse r1 = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertEquals("1", r1.getCampaignId());

        Map<String, Integer> m = new HashMap<>();
        m.put("1", 11);

        sq.setImpressionHistory(m);

        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 1, null).execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        r1 = (AdUnitResponse) i.getResponse().get(0);
        Assert.assertEquals("1", r1.getCampaignId());

        deleteAdUnitsByIds("1");
    }
}
