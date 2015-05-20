package com.dailymotion.pixelle.deserver; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.processor.DeExceptionMapper;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.dailymotion.pixelle.deserver.processor.service.CacheService;
import com.dailymotion.pixelle.deserver.servlets.DEServlet;
import com.dailymotion.pixelle.deserver.servlets.DeServletModule;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.contrib.servopublisher.HystrixServoMetricsPublisher;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.squarespace.jersey2.guice.BootstrapUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class StartServer {
    private static final DoubleGauge videoCacheHitRate = new DoubleGauge(MonitorConfig.builder("videoCacheHitRate_gauge").build());
    private static final DoubleGauge channelCacheHitRate = new DoubleGauge(MonitorConfig.builder("channelCacheHitRate_gauge").build());
    private static final DoubleGauge channelCacheLoadExceptionRate = new DoubleGauge(MonitorConfig.builder("channelCacheLoadExceptionRate_gauge").build());
    private static final DoubleGauge videoCacheLoadExceptionRate = new DoubleGauge(MonitorConfig.builder("videoCacheLoadExceptionRate_gauge").build());
    private static final LongGauge videoCacheEvictionCount = new LongGauge(MonitorConfig.builder("videoCacheEvictionCount_gauge").build());
    private static final LongGauge channelCacheEvictionCount = new LongGauge(MonitorConfig.builder("channelCacheEvictionCount_gauge").build());
    private static Logger logger = LoggerFactory.getLogger(DEServlet.class);

    static {
        DefaultMonitorRegistry.getInstance().register(videoCacheHitRate);
        DefaultMonitorRegistry.getInstance().register(channelCacheHitRate);
        DefaultMonitorRegistry.getInstance().register(channelCacheLoadExceptionRate);
        DefaultMonitorRegistry.getInstance().register(videoCacheLoadExceptionRate);
        DefaultMonitorRegistry.getInstance().register(videoCacheEvictionCount);
        DefaultMonitorRegistry.getInstance().register(channelCacheEvictionCount);
    }

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
        config.register(DeExceptionMapper.class);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorService.schedule(() -> {
            channelCacheHitRate.set(CacheService.getChannelVideosCache().stats().hitRate());
            videoCacheHitRate.set(CacheService.getOrganicVideosCache().stats().hitRate());
            channelCacheLoadExceptionRate.set(CacheService.getChannelVideosCache().stats().loadExceptionRate());
            videoCacheLoadExceptionRate.set(CacheService.getOrganicVideosCache().stats().loadExceptionRate());
            videoCacheEvictionCount.set(CacheService.getOrganicVideosCache().stats().evictionCount());
            channelCacheEvictionCount.set(CacheService.getChannelVideosCache().stats().evictionCount());
        }, 30, TimeUnit.SECONDS);

        ServletContainer servletContainer = new ServletContainer(config);
        ServletHolder sh = new ServletHolder(servletContainer);

        // Setup Threadpool
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(1000);
        threadPool.setMinThreads(100);
        threadPool.setName("de_pool");


        Server server = new Server(threadPool);
        ServletContextHandler context = new ServletContextHandler(server, "/");
        context.addServlet(DefaultServlet.class, "/");

        FilterHolder filterHolder = new FilterHolder(GuiceFilter.class);
        context.addFilter(filterHolder, "/*",
                EnumSet.allOf(DispatcherType.class));

        context.addServlet(sh, "/*");
        server.setHandler(context);

        // Extra options
        server.setDumpAfterStart(false);
        server.setDumpBeforeStop(false);
        server.setStopAtShutdown(true);

        // jmx
        MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        server.addEventListener(mbContainer);
        server.addBean(mbContainer);

        // === jetty-http.xml ===
        // HTTP Configuration
        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setOutputBufferSize(32768);
        http_config.setRequestHeaderSize(8192);
        http_config.setResponseHeaderSize(8192);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);

        ServerConnector http = new ServerConnector(server,
                new HttpConnectionFactory(http_config));
        http.setPort(DeHelper.dePort.get());
        http.setIdleTimeout(30000);
        server.addConnector(http);
        // === jetty-stats.xml ===
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(server.getHandler());
        server.setHandler(stats);

/*  Enable if request logging is required

        // === jetty-requestlog.xml ===
        NCSARequestLog requestLog = new NCSARequestLog();
        requestLog.setFilename(jetty_home + "/logs/yyyy_mm_dd.request.log");
        requestLog.setFilenameDateFormat("yyyy_MM_dd");
        requestLog.setRetainDays(90);
        requestLog.setAppend(true);
        requestLog.setExtended(true);
        requestLog.setLogCookies(false);
        requestLog.setLogTimeZone("GMT");
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog(requestLog);
        handlers.addHandler(requestLogHandler);
*/


        // === jetty-lowresources.xml ===
        LowResourceMonitor lowResourcesMonitor = new LowResourceMonitor(server);
        lowResourcesMonitor.setPeriod(1000);
        lowResourcesMonitor.setLowResourcesIdleTimeout(200);
        lowResourcesMonitor.setMonitorThreads(true);
        lowResourcesMonitor.setMaxConnections(0);
        lowResourcesMonitor.setMaxMemory(0);
        lowResourcesMonitor.setMaxLowResourcesTime(5000);
        server.addBean(lowResourcesMonitor);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    logger.info("Server shutting down...");
                    scheduledExecutorService.shutdownNow();
                    server.stop();
                } catch (Exception e) {
                    LoggerFactory.getLogger(getClass()).error("Can not stop the Jetty server", e);
                }
            }
        });

        try {
            server.start();
            server.join();
        } catch (Exception err) {
            throw new IOException(err);
        }

    }
}
