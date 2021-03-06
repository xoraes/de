package com.dailymotion.pixelle.de;

import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.dailymotion.pixelle.de.processor.DEProcessor;
import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.VideoProcessor;
import com.dailymotion.pixelle.de.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.de.processor.hystrix.VideoBulkInsertCommand;
import com.dailymotion.pixelle.de.providers.ESTestNodeClientProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dailymotion.pixelle.de.ESAdUnitsIntegrationTest.createAdUnitDataMap;
import static com.dailymotion.pixelle.de.ESAdUnitsIntegrationTest.deleteAdUnitsByIds;
import static com.dailymotion.pixelle.de.ESAdUnitsIntegrationTest.loadAdUnitMaps;
import static com.dailymotion.pixelle.de.processor.DEProcessor.deleteById;
import static com.dailymotion.pixelle.de.processor.DEProcessor.deleteIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMAT.INWIDGET;
import static com.dailymotion.pixelle.de.processor.DeHelper.currentUTCTimeString;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.google.inject.Guice.createInjector;
import static com.netflix.config.ConfigurationManager.loadCascadedPropertiesFromResources;
import static java.lang.System.out;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/12/14.
 */
public class ESVideoIntegrationTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = getLogger(ESVideoIntegrationTest.class);
    private static Injector injector;

    @BeforeClass
    public static void setUp() throws Exception {
        loadCascadedPropertiesFromResources("application");
        out.println("Running Setup");
        Injector injector = createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Client.class).toProvider(ESTestNodeClientProvider.class).asEagerSingleton();
                bind(AdUnitProcessor.class).asEagerSingleton();
                bind(VideoProcessor.class).asEagerSingleton();
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
    }

    private static void deleteVideosByIds(String... ids) throws DeException {
        for (String id : ids) {
            assertTrue(deleteById(organicIndex.get(), videosType.get(), id));
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("Deleting all known indices");
        deleteIndex(organicIndex.get());
        deleteIndex(promotedIndex.get());
        injector = null;
    }

    private static void loadVideoMaps(Map<String, Object>... map) throws Exception {
        String json;
        Video video;
        List<Video> videos = new ArrayList<Video>();
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
                videos.add(video);
            }
        }
        new VideoBulkInsertCommand(videos).execute();
        sleep(2000);
    }

    private static Map<String, Object> createVideoDataMap(String id) {
        String timeNow = currentUTCTimeString();
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("_id", id);
        m.put("_updated", timeNow);
        m.put("_created", timeNow);
        m.put("publication_date", timeNow);
        m.put("categories", new ArrayList<String>(asList("cat1", "cat2")));
        m.put("languages", new ArrayList<String>(asList("en", "fr")));
        m.put("tags", new ArrayList<String>(asList("tag1", "tag2")));
        m.put("title", "title");
        m.put("description", "description");
        m.put("channel", "channel");
        m.put("channel_name", "channel_name");
        m.put("channel_id", "channel_id");
        m.put("channel_tier", "channel_tier");
        m.put("resizable_thumbnail_url", "resizable_thumbnail_url");
        m.put("duration", 123);
        return m;
    }

    @Test
    public void testGetAdsAndVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        loadVideoMaps(m1, m2, m3);

        Map m4 = createAdUnitDataMap("1", "1");
        Map m5 = createAdUnitDataMap("2", "2");
        Map m6 = createAdUnitDataMap("3", "3");
        loadAdUnitMaps(m4, m5, m6);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 6, "promoted,organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertEquals(6, i.getResponse().size());

        out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 6, "promoted").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertEquals(3, i.getResponse().size());

        out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 6, "organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertEquals(3, i.getResponse().size());

        out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 3, "promoted,organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        out.println(i.getResponse().get(2).getClass().getName());
        assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("AdUnit"));

        deleteVideosByIds("1", "2", "3");
        deleteAdUnitsByIds("1", "2", "3");
    }

    @Test
    public void testPattern() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        Map m4 = createVideoDataMap("4");
        loadVideoMaps(m1, m2, m3, m4);

        Map m5 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m5);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setPattern("POO");

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, "promoted,organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 3);
        assertTrue(i.getResponse().get(0).getClass().getCanonicalName().contains("AdUnit"));
        assertTrue(i.getResponse().get(1).getClass().getCanonicalName().contains("Video"));
        assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("Video"));
        deleteVideosByIds("1", "2", "3", "4");
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testFillWithTargettedVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        Map m4 = createVideoDataMap("4");
        loadVideoMaps(m1, m2, m3, m4);

        Map m5 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m5);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 5, "promoted,organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 5);

        deleteVideosByIds("1", "2", "3", "4");
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testFillWithUntargetedVideo() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        loadVideoMaps(m1, m2, m3);

        Map m4 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m4);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("ttt")));
        sq.setDevice("ved");
        sq.setFormat("ved");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 6, "promoted,organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 3);
        deleteVideosByIds("1", "2", "3");
        deleteAdUnitsByIds("1");
    }

    @Test
    public void testCheckAllVideoFields() throws Exception {
        Map m1 = createVideoDataMap("1");
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        ItemsResponse i = new QueryCommand(sq, 1, "organic").execute();
        assertNotNull(i);
        assertEquals(1, i.getResponse().size());
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        assertEquals("1", video.getVideoId());
        assertEquals("channel_tier", video.getChannelTier());
        assertEquals("channel", video.getChannel());
        assertEquals("channel_name", video.getChannelName());
        assertEquals("organic", video.getContentType());
        assertEquals("description", video.getDescription());
        assertEquals("title", video.getTitle());
        assertTrue(video.getDuration() == 123);
        assertEquals("resizable_thumbnail_url", video.getResizableThumbnailUrl());
        deleteVideosByIds("1");
    }

    @Test
    public void testNullField() throws Exception {
        Map m1 = createVideoDataMap("1");
        m1.put("channel_name", null);
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        ItemsResponse i = new QueryCommand(sq, 1, "organic").execute();
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 1);
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        assertNull(video.getChannelName());
        deleteVideosByIds("1");
    }

    @Test
    public void testNullResizeThumbnailUrl() throws Exception {
        Map m1 = createVideoDataMap("1");
        m1.put("resizable_thumbnail_url", null);
        loadVideoMaps(m1);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setTime("2014-11-21T01:00:00Z");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        ItemsResponse i = new QueryCommand(sq, 1, "organic").execute();
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 0);
        if (i.getResponse().size() > 0) {
            deleteVideosByIds("1");
        }
    }

    @Test
    public void testKeywordsSearchWithShingles() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        m3.put("title", "ONE TWO THREE");
        m2.put("title", "TWO FOUR ONE");
        m1.put("title", "THREE TWO");


        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setDebugEnabled(true);
        Map<String, Float> m = new HashMap<>();
        m.put("one two", null);
        sq.setKeywords(m);

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, "organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 3);
        VideoResponse r1 = (VideoResponse) i.getResponse().get(0);
        VideoResponse r2 = (VideoResponse) i.getResponse().get(1);
        VideoResponse r3 = (VideoResponse) i.getResponse().get(2);
        Assert.assertEquals("3", r1.getVideoId());
        Assert.assertEquals("2", r2.getVideoId());
        Assert.assertEquals("1", r3.getVideoId());
        deleteVideosByIds("1", "2", "3");
    }

    @Test
    public void testKeywordsSearchWithScore() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        m1.put("title", "lala");
        m1.put("description", "lala");
        m3.put("title", "mankind longggggggggg field");
        m2.put("title", "nokey");
        m3.put("description", "mankind longggggggggg field");
        m2.put("description", "nokey");


        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setDebugEnabled(true);
        Map<String, Float> m = new HashMap<>();
        m.put("mankind", 0.3f);
        m.put("nokey", 0.2f);
        m.put("lala", 0.1f);
        sq.setKeywords(m);

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, "organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 3);
        VideoResponse r1 = (VideoResponse) i.getResponse().get(0);
        VideoResponse r2 = (VideoResponse) i.getResponse().get(1);
        VideoResponse r3 = (VideoResponse) i.getResponse().get(2);
        Assert.assertEquals("3", r1.getVideoId());
        Assert.assertEquals("2", r2.getVideoId());
        Assert.assertEquals("1", r3.getVideoId());
        deleteVideosByIds("1", "2", "3");
    }

    @Test
    public void testKeywordsSearchWithNullScore() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");

        m3.put("title", "grapeshot");
        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        Map<String, Float> m = new HashMap<>();
        m.put("grapeshot", null);
        sq.setKeywords(m);

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, "organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 1);
        VideoResponse r1 = (VideoResponse) i.getResponse().get(0);
        Assert.assertEquals("3", r1.getVideoId());
        deleteVideosByIds("1", "2", "3");
    }

    @Test
    public void testGetMultipleVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");

        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, "organic").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 3);
        deleteVideosByIds("1", "2", "3");
    }

    //fallback on english
    @Test
    public void testFallbackToEnglish() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        m1.put("categories", new ArrayList<String>(asList("assd")));
        m2.put("categories", new ArrayList<String>(asList("asd")));
        m3.put("categories", new ArrayList<String>(asList("asd")));
        m1.put("languages", new ArrayList<String>(asList("en")));
        m2.put("languages", new ArrayList<String>(asList("en")));
        m3.put("languages", new ArrayList<String>(asList("hi")));

        loadVideoMaps(m1, m2, m3);
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("fr")));
        sq.setLocations(new ArrayList<String>(asList("fr")));

        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 10, "organic").execute();
        out.println("Language Response ====>:" + i.toString());
        assertNotNull(i);
        assertTrue(i.getResponse().size() == 2);
        deleteVideosByIds("1", "2", "3");
    }
}
