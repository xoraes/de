package com.dailymotion.pixelle.de.processor;

import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.common.services.CacheService;
import com.dailymotion.pixelle.de.providers.ESTestNodeClientProvider;
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

import static com.dailymotion.pixelle.common.services.CacheService.getChannelVideosCache;
import static com.dailymotion.pixelle.de.processor.DEProcessor.deleteIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMAT;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMAT.INWIDGET;
import static com.dailymotion.pixelle.de.processor.DeHelper.channelIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.ESAdUnitsIntegrationTest.createAdUnitDataMap;
import static com.dailymotion.pixelle.de.processor.ESAdUnitsIntegrationTest.deleteAdUnitsByIds;
import static com.dailymotion.pixelle.de.processor.ESAdUnitsIntegrationTest.loadAdUnitMaps;
import static com.google.inject.Guice.createInjector;
import static com.netflix.config.ConfigurationManager.loadCascadedPropertiesFromResources;
import static java.lang.System.out;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/5/15.
 */
public class ESChannelVideoIntegrationTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOGGER = getLogger(ESChannelVideoIntegrationTest.class);
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
                bind(ChannelProcessor.class).asEagerSingleton();
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LOGGER.info("Deleting all known indices");
        deleteIndex(organicIndex.get());
        deleteIndex(promotedIndex.get());
        deleteIndex(channelIndex.get());
        injector = null;
    }

    @Test
    public void testDmApiCall() throws Exception {

        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setChannels(asList("buzzfeedvideo", "spi0n"));
        sq.setFormat(INWIDGET.toString());
        sq.setSortOrder("recent");
        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        out.println("Response ====>:" + i.toString());

        assertNotNull(i);
        assertEquals(1, i.getResponse().size());
        VideoResponse video = (VideoResponse) i.getResponse().get(0);
        out.println(video.toString());
        assertNotNull(video.getVideoId());
        assertNotNull(video.getChannel());
        assertNotNull(video.getChannelId());
        assertNotNull(video.getChannelName());
        assertNotNull(video.getContentType());
        assertNotNull(video.getDescription());
        assertNotNull(video.getTitle());
        assertNotNull(video.getDuration());
        assertNotNull(video.getResizableThumbnailUrl());

        Map m4 = createAdUnitDataMap("1", "1");
        loadAdUnitMaps(m4);

        sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setChannels(asList("buzzfeedvideo", "spi0n"));
        sq.setSortOrder("recent");

        out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 3, "promoted,channel").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertEquals(3, i.getResponse().size());
        assertTrue(i.getResponse().get(2).getClass().getCanonicalName().contains("AdUnit"));
        assertEquals(1, getChannelVideosCache().size());
        assertEquals(1, getChannelVideosCache().stats().hitCount());

        sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setChannel("buzzfeedvideo");


        out.println("Search Query ====>" + sq.toString());
        i = new QueryCommand(sq, 1, "promoted,channel").execute();
        out.println("Response ====>:" + i.toString());
        assertNotNull(i);
        assertEquals(1, i.getResponse().size());
        assertEquals(2, getChannelVideosCache().size());
        assertEquals(1, getChannelVideosCache().stats().hitCount());

        deleteAdUnitsByIds("1");
    }

    @Test(expected = HystrixBadRequestException.class)
    public void testTooManyChannels() throws Exception {
        SearchQueryRequest sq = new SearchQueryRequest();
        sq.setTime("2014-12-31T15:00:00-0800");
        sq.setCategories(new ArrayList(asList("cat1")));
        sq.setDevice("dev1");
        sq.setFormat(INWIDGET.toString());
        sq.setLanguages(new ArrayList<String>(asList("en")));
        sq.setLocations(new ArrayList<String>(asList("us")));
        sq.setChannels(asList("buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n", "buzzfeedvideo", "spi0n"));


        out.println("Search Query ====>" + sq.toString());
        ItemsResponse i = new QueryCommand(sq, 1, "promoted,channel").execute();
        out.println("Response ====>:" + i.toString());
    }
}
