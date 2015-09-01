package com.dailymotion.pixelle.common.server;

import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.dailymotion.pixelle.de.processor.ChannelProcessor;
import com.dailymotion.pixelle.de.processor.DEProcessor;
import com.dailymotion.pixelle.de.processor.VideoProcessor;
import com.dailymotion.pixelle.de.providers.ESNodeClientProvider;
import com.dailymotion.pixelle.forecast.processor.Forecaster;
import com.google.inject.servlet.ServletModule;
import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet;
import org.elasticsearch.client.Client;

/**
 * Created by n.dhupia on 2/3/15.
 */
public class AppServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        // Must configure at least one JAX-RS resource or the
        // server will fail to start.
        bind(Client.class).toProvider(ESNodeClientProvider.class).asEagerSingleton();
        bind(AdUnitProcessor.class).asEagerSingleton();
        bind(Forecaster.class).asEagerSingleton();
        bind(VideoProcessor.class).asEagerSingleton();
        bind(ChannelProcessor.class).asEagerSingleton();
        bind(DEProcessor.class).asEagerSingleton();
        bind(AppServlet.class).asEagerSingleton();
        bind(HystrixMetricsStreamServlet.class).asEagerSingleton();
        serve("/hystrix.stream").with(HystrixMetricsStreamServlet.class);
    }
}
