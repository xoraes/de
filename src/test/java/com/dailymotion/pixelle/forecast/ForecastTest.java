package com.dailymotion.pixelle.forecast;

import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.dailymotion.pixelle.de.processor.DEProcessor;
import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.DeHelper;
import com.dailymotion.pixelle.de.processor.ESAdUnitsIntegrationTest;
import com.dailymotion.pixelle.forecast.model.ForecastRequest;
import com.dailymotion.pixelle.forecast.model.ForecastResponse;
import com.dailymotion.pixelle.common.services.CacheService;
import com.dailymotion.pixelle.de.providers.ESTestNodeClientProvider;
import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.dailymotion.pixelle.forecast.processor.Forecaster;
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
import java.util.Map;

/**
 * Created by n.dhupia on 8/12/15.
 */
public class ForecastTest {
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
                bind(DEProcessor.class).asEagerSingleton();
            }
        });
        //warm up cache
        CacheService.getPerCountryCountCache().get(DeHelper.CATEGORIESBYCOUNTRY);
        CacheService.getPerCountryCountCache().get(DeHelper.DEVICESBYCOUNTRY);
        CacheService.getPerCountryCountCache().get(DeHelper.FORMATSBYCOUNTRY);
        CacheService.getPerCountryCountCache().get(DeHelper.EVENTSBYCOUNTRY);
        CacheService.getPerCountryCountCache().get(DeHelper.LANGUAGEBYCOUNTRY);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("Deleting all known indices");
        DEProcessor.deleteIndex(DeHelper.promotedIndex.get());

    }

    @Test
    public void noop() throws Exception {

        System.out.println(CacheService.getCountryLangCountCache().toString());
        System.out.println(CacheService.getCountryDeviceCountCache().toString());
        System.out.println(CacheService.getCountryFormatCountCache().toString());
        System.out.println(CacheService.getCountryCategoryCountCache().toString());
        Long res = 0l;
        Long total = CacheService.getCountryCategoryCountCache().get("total", "total");
        res = CacheService.getCountryCategoryCountCache().get("total", "fun");

        System.out.println(res);
        System.out.println(total);
    }

    @Test
    public void testForecastWithoutEndDate() throws Exception {
        Map m1 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m2 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        m1.put("cpv", "10");
        m2.put("cpv", "20");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m1, m2);

        ForecastRequest req = new ForecastRequest();

        req.setLocations(new ArrayList<>(Arrays.asList("us")));
        req.setCpv(15);
        req.setStartDate("2015-01-01T00:00:00Z");
        Integer[] sch = {16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215};
        req.setSchedules(sch);

        ForecastResponse response = Forecaster.forecast(req);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMaxViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMinViews() > 0);
        Assert.assertNull(response.getForecastViewsList().get(0).getTotalMaxViews());
        Assert.assertNull(response.getForecastViewsList().get(0).getTotalMinViews());
        System.out.println(response.toString());
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

        req.setLocations(new ArrayList<>(Arrays.asList("us", "fr")));
        req.setCpv(15);
        req.setEndDate("2015-10-30T00:00:00Z");
        req.setStartDate("2015-01-01T00:00:00Z");
        req.setDevices(Arrays.asList("desktop", "tablet", "tv"));
        req.setLanguages(Arrays.asList("en", "fr"));

        Integer[] sch = {16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215};
        req.setSchedules(sch);

        ForecastResponse response = Forecaster.forecast(req);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMinViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMinViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getTotalMaxViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getTotalMinViews() > 0);
        System.out.println(response.toString());
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1", "2");
    }

    @Test
    public void testForecastNoLocation() throws Exception {
        Map m1 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m2 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        m1.put("cpv", "10");
        m2.put("cpv", "20");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m1, m2);

        ForecastRequest req = new ForecastRequest();
        req.setCpv(15);
        req.setEndDate("2015-10-30T00:00:00Z");
        req.setStartDate("2015-01-01T00:00:00Z");

        ForecastResponse response = Forecaster.forecast(req);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMaxViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getDailyMinViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getTotalMaxViews() > 0);
        Assert.assertTrue(response.getForecastViewsList().get(0).getTotalMinViews() > 0);

        System.out.println(response.toString());

        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1", "2");
    }

    @Test(expected = ForecastException.class)
    public void testForecastNoCpv() throws Exception {
        Map m1 = ESAdUnitsIntegrationTest.createAdUnitDataMap("1", "1");
        Map m2 = ESAdUnitsIntegrationTest.createAdUnitDataMap("2", "2");
        m1.put("cpv", "10");
        m2.put("cpv", "20");
        ESAdUnitsIntegrationTest.loadAdUnitMaps(m1, m2);
        ForecastRequest req = new ForecastRequest();
        String countries[] = new String[]{"US"};
        req.setLocations(Arrays.asList(countries));
        ForecastResponse response = Forecaster.forecast(req);
        ESAdUnitsIntegrationTest.deleteAdUnitsByIds("1", "2");
    }

}
