package com.dailymotion.pixelle.deserver; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.logger.Slf4jTypeListener;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.dailymotion.pixelle.deserver.processor.DEProcessorImpl;
import com.dailymotion.pixelle.deserver.providers.ESNodeClientProvider;
import com.dailymotion.pixelle.deserver.servlets.DEServlet;
import com.dailymotion.pixelle.deserver.servlets.HealthCheck;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.elasticsearch.client.Client;

public class DEServerContextListener extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new JerseyServletModule() {
            @Override
            protected void configureServlets() {
                bindListener(Matchers.any(), new Slf4jTypeListener());
                // Must configure at least one JAX-RS resource or the
                // server will fail to start.
                bind(Client.class).toProvider(ESNodeClientProvider.class).asEagerSingleton();
                bind(HealthCheck.class).asEagerSingleton();
                bind(DEProcessor.class).to(DEProcessorImpl.class).asEagerSingleton();
                bind(DEServlet.class).asEagerSingleton();
                // Route all requests through GuiceContainer
                serve("/*").with(GuiceContainer.class);
            }
        });
    }
}
