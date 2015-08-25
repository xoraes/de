package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.deserver.processor.service.CacheService;
import com.dailymotion.pixelle.deserver.providers.ESTestNodeClientProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by n.dhupia on 3/5/15.
 */
public class ESChannelVideoIntegrationTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ESChannelVideoIntegrationTest.class);
    private static Injector injector;

    @BeforeClass
    public static void setUp() throws Exception {
        ConfigurationManager.loadCascadedPropertiesFromResources("application");
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

    @Test
    public void testDmApiCall() throws Exception {

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setChannels(Arrays.asList("buzzfeedvideo", "spi0n"));
        sq.setSortOrder("recent");
        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        System.out.println("Response ====>:" + i.toString());

        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        System.out.println(video.toString());
        Assert.assertNotNull(video.getVideoId());
        Assert.assertNotNull(video.getChannel());
        Assert.assertNotNull(video.getChannelId());
        Assert.assertNotNull(video.getChannelName());
        Assert.assertNotNull(video.getContentType());
        Assert.assertNotNull(video.getDescription());
        Assert.assertNotNull(video.getTitle());
        Assert.assertNotNull(video.getDuration());
        Assert.assertNotNull(video.getResizableThumbnailUrl());

        Map m4 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m4);

        sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setChannels(Arrays.asList("buzzfeedvideo", "spi0n"));
        sq.setSortOrder("recent");

        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 3, "promoted,channel").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(3, i.getResponse().size());
        Assert.assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("AdUnit"));
        Assert.assertEquals(1, CacheService.getChannelVideosCache().size());
        Assert.assertEquals(1, CacheService.getChannelVideosCache().stats().hitCount());

        sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setChannel("buzzfeedvideo");


        System.out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 1, "promoted,channel").execute();
        System.out.println("Response ====>:" + i.toString());
        Assert.assertNotNull(i);
        Assert.assertEquals(1, i.getResponse().size());
        Assert.assertEquals(2, CacheService.getChannelVideosCache().size());
        Assert.assertEquals(1, CacheService.getChannelVideosCache().stats().hitCount());

        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1");
    }

    @Test(expected = HystrixBadRequestException.class)
    public void testTooManyChannels() throws Exception {
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(Arrays.asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat("fmt1");
        sq.setLanguages(new ArrayList<String>(Arrays.asList("en")));
        sq.setLocations(new ArrayList<String>(Arrays.asList("us")));
        sq.setChannels(Arrays.asList("buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n"));


        System.out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        System.out.println("Response ====>:" + i.toString());
    }
}
