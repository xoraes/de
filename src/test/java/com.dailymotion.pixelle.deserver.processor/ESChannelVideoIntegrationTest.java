package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelVideoBulkInsertCommand;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by n.dhupia on 3/5/15.
 */
public class ESChannelVideoIntegrationTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static Injector injector;
    private static Logger logger = LoggerFactory.getLogger(ESChannelVideoIntegrationTest.class);

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
                bind(ChannelProcessor.class).asEagerSingleton();
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("Deleting all known indices");
        DEProcessor.deleteIndex(DeHelper.organicIndex.get());
        DEProcessor.deleteIndex(DeHelper.promotedIndex.get());
        DEProcessor.deleteIndex(DeHelper.channelIndex.get());
        injector = null;
    }

    public static void loadVideoMaps(Map<String, Object>... map) throws Exception {
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
        new ChannelVideoBulkInsertCommand(videos).execute();
        Thread.sleep(2000);
    }

    public static Map<String, Object> createVideoDataMap(String id) {
        String timeNow = DeHelper.currentUTCTimeString();
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("_id", id);
        m.put("video_id", id);
        m.put("_updated", timeNow);
        m.put("_created", timeNow);
        m.put("publication_date", timeNow);
        m.put("categories", new ArrayList<String>(Arrays.asList("cat1", "cat2")));
        m.put("languages", new ArrayList<String>(Arrays.asList("en", "fr")));
        m.put("tags", new ArrayList<String>(Arrays.asList("tag1", "tag2")));
        m.put("title", "title");
        m.put("description", "description");
        m.put("channel", "channel");
        m.put("channel_name", "channel_name");
        m.put("channel_id", "channel_id");
        m.put("resizable_thumbnail_url", "resizable_thumbnail_url");
        m.put("duration", 123);
        return m;
    }

    public static void deleteVideosByIds(String... ids) throws Exception {
        for (String id : ids) {
            System.out.println("Deleting Video Id: " + id);
            Assert.assertTrue(DEProcessor.deleteById(DeHelper.channelIndex.get(), DeHelper.videosType.get(), id));
        }
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
        sq.setChannel("channel");

        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        System.out.println("Video" + video.toString());
        Assert.assertTrue(video.getVideoId().equalsIgnoreCase("1"));
        Assert.assertTrue(video.getChannel().equals("channel"));
        Assert.assertTrue(video.getChannelId().equals("channel_id"));
        Assert.assertTrue(video.getChannelName().equals("channel_name"));
        Assert.assertTrue(video.getContentType().equals("organic"));
        Assert.assertTrue(video.getDescription().equals("description"));
        Assert.assertTrue(video.getTitle().equals("title"));
        Assert.assertTrue(video.getDuration() == 123);
        Assert.assertTrue(video.getResizableThumbnailUrl().equals("resizable_thumbnail_url"));
        deleteVideosByIds("1");
    }

    @Test
    public void testDmApiCall() throws Exception {
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setChannel("rs");

        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        Assert.assertNotNull(i);
        Assert.assertTrue(i.getResponse().size() == 1);
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        System.out.println("Video" + video.toString());
        Assert.assertNotNull(video.getVideoId());
        Assert.assertNotNull(video.getChannel());
        Assert.assertNotNull(video.getChannelId());

        Assert.assertNotNull(video.getContentType());
        Assert.assertNotNull(video.getDescription());
        Assert.assertNotNull(video.getTitle());
        Assert.assertNotNull(video.getDuration());
        Assert.assertNotNull(video.getResizableThumbnailUrl());


    }


    @Test
    public void testGetAdsAndVideos() throws Exception {
        Map m1 = createVideoDataMap("1");
        Map m2 = createVideoDataMap("2");
        Map m3 = createVideoDataMap("3");
        loadVideoMaps(m1, m2, m3);

        Map m4 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m4);


        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setChannel("channel");

        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 3, "promoted,channel").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        System.out.println(i.getResponse().get(2).getClass().getName());
        Assert.assertTrue(i.getResponse().size() == 3);
        Assert.assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("AdUnit"));

        deleteVideosByIds("1", "2", "3");
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1");
    }
}
