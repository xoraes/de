package com.dailymotion.pixelle.deserver; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.dailymotion.pixelle.deserver.servlets.DEServlet;
import com.dailymotion.pixelle.deserver.servlets.DeServletModule;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.contrib.servopublisher.HystrixServoMetricsPublisher;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.squarespace.jersey2.guice.BootstrapUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;

public final class StartServer {

    private StartServer() {
    }

    public static void main(String[] args) throws Exception {

        System.setProperty(DynamicPropertyFactory.ENABLE_JMX, "true");
        System.setProperty("archaius.configurationSource.additionalUrls", "file:///etc/de.conf");
        String env = System.getProperty("env");
        if (!StringUtils.isBlank(env)) {
            System.setProperty("archaius.deployment.environment", env);
        }
        ConfigurationManager.loadCascadedPropertiesFromResources("de");
        HystrixPlugins.getInstance().registerMetricsPublisher(HystrixServoMetricsPublisher.getInstance());


        ServiceLocator locator = BootstrapUtils.newServiceLocator();
        BootstrapUtils.newInjector(locator, Arrays.asList(new DeServletModule()));

        BootstrapUtils.install(locator);

        ResourceConfig config = new ResourceConfig();

        config.register(MultiPartFeature.class);
        config.register(DEServlet.class);


        ServletContainer servletContainer = new ServletContainer(config);

        ServletHolder sh = new ServletHolder(servletContainer);
        Server server = new Server(DeHelper.getPort());
        ServletContextHandler context = new ServletContextHandler(server, "/");
        context.addServlet(DefaultServlet.class, "/");

        FilterHolder filterHolder = new FilterHolder(GuiceFilter.class);
        context.addFilter(filterHolder, "/*",
                EnumSet.allOf(DispatcherType.class));

        context.addServlet(sh, "/*");
        server.setHandler(context);

        try {
            server.start();
            server.join();
        } catch (Exception err) {
            throw new IOException(err);
        }
    }
}
