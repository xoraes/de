package com.dailymotion.pixelle.deserver.servlets;

import com.dailymotion.pixelle.deserver.processor.AdUnitProcessor;
import com.dailymotion.pixelle.deserver.processor.ChannelProcessor;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.dailymotion.pixelle.deserver.processor.VideoProcessor;
import com.dailymotion.pixelle.deserver.providers.ESNodeClientProvider;
import com.google.inject.servlet.ServletModule;
import com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet;
import org.elasticsearch.client.Client;

/**
 * Created by n.dhupia on 2/3/15.
 */
public class DeServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        // Must configure at least one JAX-RS resource or the
        // server will fail to start.
        bind(Client.class).toProvider(ESNodeClientProvider.class).asEagerSingleton();
        bind(AdUnitProcessor.class).asEagerSingleton();
        bind(VideoProcessor.class).asEagerSingleton();
        bind(ChannelProcessor.class).asEagerSingleton();
        bind(DEProcessor.class).asEagerSingleton();
        bind(DEServlet.class).asEagerSingleton();
        bind(HystrixMetricsStreamServlet.class).asEagerSingleton();
        serve("/hystrix.stream").with(HystrixMetricsStreamServlet.class);
    }
}
