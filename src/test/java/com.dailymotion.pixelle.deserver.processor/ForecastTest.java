package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ForecastRequest;
import com.dailymotion.pixelle.deserver.model.ForecastResponse;
import com.dailymotion.pixelle.deserver.providers.ESTestNodeClientProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by n.dhupia on 8/12/15.
 */
public class ForecastTest {
    @BeforeClass
    public static void setUp() throws Exception {
        ConfigurationManager.loadCascadedPropertiesFromResources("de");
        System.out.println("Running Setup");
        Injector injector = Guice.createInjector(new AbstractModule() {

            @Override
            protected void configure() {
                bind(Client.class).toProvider(ESTestNodeClientProvider.class).asEagerSingleton();
                bind(AdUnitProcessor.class).asEagerSingleton();
                bind(Forecaster.class).asEagerSingleton();
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("Deleting all known indices");
        DEProcessor.deleteIndex(DeHelper.promotedIndex.get());

    }

    @Test
    public void testForecastWithoutEndDate() throws Exception {
        Map m1 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m2 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        m1.put("cpv", "10");
        m2.put("cpv", "20");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m1, m2);

        ForecastRequest req = new ForecastRequest();
        String countries[] = new String[]{"US"};
        req.setLocations(Arrays.asList(countries));
        req.setCpv(15L);
        req.setStartDate("2015-01-01T00:00:00Z");
        Integer[] sch = {16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215};
        req.setSchedules(sch);

        ForecastResponse response = DEProcessor.forecast(req);
        Assert.assertTrue(response.getDailyMaxViews() > 0);
        Assert.assertTrue(response.getDailyMinViews() > 0);
        Assert.assertNull(response.getTotalMaxViews());
        Assert.assertNull(response.getTotalMinViews());

        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1", "2");
    }
    @Test
    public void testForecastWithEndDate() throws Exception {
        Map m1 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m2 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        m1.put("cpv", "10");
        m2.put("cpv", "20");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m1, m2);

        ForecastRequest req = new ForecastRequest();
        String countries[] = new String[]{"US"};
        req.setLocations(Arrays.asList(countries));
        req.setCpv(15L);
        req.setEndDate("2015-10-30T00:00:00Z");
        req.setStartDate("2015-01-01T00:00:00Z");
        req.setDevices(Arrays.asList("desktop","tablet","tv"));

        Integer[] sch = {16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215};
        req.setSchedules(sch);

        ForecastResponse response = DEProcessor.forecast(req);
        Assert.assertTrue(response.getDailyMaxViews() > 0);
        Assert.assertTrue(response.getDailyMinViews() > 0);
        Assert.assertTrue(response.getTotalMaxViews() > 0);
        Assert.assertTrue(response.getTotalMinViews() > 0);

        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1", "2");
    }
}
