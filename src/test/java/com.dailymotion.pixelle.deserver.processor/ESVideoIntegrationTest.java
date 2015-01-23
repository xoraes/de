package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.VideoInsertCommand;
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

/**
 * Created by n.dhupia on 12/12/14.
 */
public class ESVideoIntegrationTest {
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
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
        es = injector.getInstance(DEProcessor.class);

    }

    public static void deleteVideosByIds(String... ids) throws Exception {
        for (String id : ids) {
            Assert.assertTrue(es.deleteById(DeHelper.getIndex(), DeHelper.getOrganicVideoType(), id));
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("Deleting the index");
        es.deleteIndex();
        injector = null;
    }

    public static void loadVideoMaps(Map<String, Object>... map) throws Exception {
        String json;
        Video video;
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
                video = mapper.readValue(json, Video.class);
                Assert.assertTrue(new VideoInsertCommand(es, video).execute());
            }

        }
        Thread.sleep(2000);
    }

    public static Map<String, Object> createVideoDataMap(String id) {
        String timeNow = DeHelper.currentUTCTimeString();
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("_id", id);
        m.put("_updated", timeNow);
        m.put("_created", timeNow);
        m.put("publication_date", timeNow);
        m.put("categories", new ArrayList<String>(Arrays.asList("cat1", "cat2")));
        m.put("languages", new ArrayList<String>(Arrays.asList("en", "fr")));
        m.put("tags", new ArrayList<String>(Arrays.asList("tag1", "tag2")));
        m.put("title", "title");
        m.put("description", "description");
        m.put("channel", "channel");
        m.put("channel_url", "channel_url");
        m.put("channel_name", "channel_name");
        m.put("channel_id", "channel_id");
        m.put("channel_tier", "channel_tier");
        m.put("resizable_thumbnail_url", "resizable_thumbnail_url");
        m.put("thumbnail_url", "thumbnail_url");
        m.put("status", "active");
        m.put("duration", 123);
        return m;
    }

    @Test
    public void testGetAdsAndVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        loadVideoMaps(m1, m2, m3);

        Map m4 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m5 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        Map m6 = ESAdUnitsIntegrationTest.createAdUnitDataMap("3", "3");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(es, m4, m5, m6);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(6);

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "promoted,organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 6);

        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(es, sq, "promoted").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);

        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(es, sq, "organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);

        sq.setPositions(3);
        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(es, sq, "promoted,organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        System.out.println(i.getResponse().get(2).getClass().getName());
        Assert.assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("AdUnit"));

        deleteVideosByIds("1", "2", "3");
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds(es, "1", "2", "3");
    }

    @Test
    public void testFillWithTargettedVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        Map m4 = createVideoDataMap("4");
        loadVideoMaps(m1, m2, m3, m4);

        Map m5 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(es, m5);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(5);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "promoted,organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 5);

        deleteVideosByIds("1", "2", "3", "4");
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds(es, "1");
    }

    @Test
    public void testFillWithUntargetedVideo() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        loadVideoMaps(m1, m2, m3);

        Map m4 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(es, m4);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("ttt")));
        sq.setDevice("ved");
        sq.setFormat("ved");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(6);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "promoted,organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);
        deleteVideosByIds("1", "2", "3");
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds(es, "1");
    }

    @Test
    public void testCheckAllVideoFields() throws Exception {
        Map m1 = createVideoDataMap("1");
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(1);
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        Assert.assertTrue(video.getVideoId().equalsIgnoreCase("1"));
        Assert.assertTrue(video.getChannelTier().equalsIgnoreCase("channel_tier"));
        Assert.assertTrue(video.getChannel().equals("channel"));
        Assert.assertTrue(video.getChannelId().equals("channel_id"));
        Assert.assertTrue(video.getChannelName().equals("channel_name"));
        Assert.assertTrue(video.getChannelUrl().equals("channel_url"));
        Assert.assertTrue(video.getContentType().equals("organic"));
        Assert.assertTrue(video.getDescription().equals("description"));
        Assert.assertTrue(video.getTitle().equals("title"));
        Assert.assertTrue(video.getDuration() == 123);
        Assert.assertTrue(video.getThumbnailUrl().equals("thumbnail_url"));
        Assert.assertTrue(video.getResizableThumbnailUrl().equals("resizable_thumbnail_url"));
        deleteVideosByIds("1");
    }

    @Test
    public void testNullField() throws Exception {
        Map m1 = createVideoDataMap("1");
        m1.put("channel_url", null);
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(1);
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        Assert.assertNull(video.getChannelUrl());
        deleteVideosByIds("1");
    }

    @Test
    public void testGetMultipleVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");

        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(10);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);
        deleteVideosByIds("1", "2", "3");
    }

    @Test
    public void testStatusInActive() throws Exception {
        Map m1 = createVideoDataMap("1");
        m1.put("status", "inactive");
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setPositions(10);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 0);
        deleteVideosByIds("1");
    }

    @Test
    public void testCategoryTargetingNegativeWithFallback() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        m1.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m2.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m3.put("languages", new ArrayList<String>(Arrays.asList("fr")));
        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("ZZZZ")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("fr")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("fr")));
        sq.setPositions(10);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        System.out.println("Language Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 3);
        deleteVideosByIds("1", "2", "3");
    }

    //fallback on english
    @Test
    public void testFallbackToEnglish() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        m1.put("categories", new ArrayList<String>(Arrays.asList("assd")));
        m2.put("categories", new ArrayList<String>(Arrays.asList("asd")));
        m3.put("categories", new ArrayList<String>(Arrays.asList("asd")));
        m1.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m2.put("languages", new ArrayList<String>(Arrays.asList("en")));
        m3.put("languages", new ArrayList<String>(Arrays.asList("hi")));

        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("fr")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("fr")));
        sq.setPositions(10);
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(es, sq, "organic").execute();
        System.out.println("Language Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 2);
        deleteVideosByIds("1", "2", "3");
    }
}