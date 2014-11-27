package com.dailymotion.pixelle.deserver; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;

public class StartServer {

    public static void main(String[] args) throws Exception {
        System.setProperty(DynamicPropertyFactory.ENABLE_JMX, "true");
        String env = System.getProperty("env");
        if (!StringUtils.isBlank(env)) {
            System.setProperty("archaius.deployment.environment", env);
        }
        ConfigurationManager.loadCascadedPropertiesFromResources("de");
        // Create the server.
        Server server = new Server(DeHelper.getPort());

        // Create a servlet context and add the jersey servlet.
        ServletContextHandler sch = new ServletContextHandler(server, "/");

        // Add our Guice listener that includes our bindings
        sch.addEventListener(new DEServerContextListener());

        // Then add GuiceFilter and configure the server to
        // reroute all requests through this filter.
        sch.addFilter(GuiceFilter.class, "/*", null);

        // Must add DefaultServlet for embedded Jetty.
        // Failing to do this will cause 404 errors.
        // This is not needed if web.xml is used instead.
        sch.addServlet(DefaultServlet.class, "/");

        // Start the server
        server.start();
        server.join();
    }
}

