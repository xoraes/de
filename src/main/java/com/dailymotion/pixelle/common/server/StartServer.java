package com.dailymotion.pixelle.common.server; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.common.exceptionmapper.JSONParseExceptionMapper;
import com.dailymotion.pixelle.de.processor.DeExceptionMapper;
import com.google.inject.servlet.GuiceFilter;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.hystrix.contrib.servopublisher.HystrixServoMetricsPublisher;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.LongGauge;
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
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static com.dailymotion.pixelle.common.services.CacheService.getChannelVideosCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryCategoryCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryDeviceCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryEventCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryFormatCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryLangCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getOrganicVideosCache;
import static com.dailymotion.pixelle.common.services.CacheService.getPerCountryCountCache;
import static com.dailymotion.pixelle.de.processor.DeHelper.dePort;
import static com.netflix.config.ConfigurationManager.loadCascadedPropertiesFromResources;
import static com.netflix.config.DynamicPropertyFactory.ENABLE_JMX;
import static com.netflix.servo.DefaultMonitorRegistry.getInstance;
import static com.netflix.servo.monitor.MonitorConfig.builder;
import static com.squarespace.jersey2.guice.BootstrapUtils.install;
import static com.squarespace.jersey2.guice.BootstrapUtils.newInjector;
import static com.squarespace.jersey2.guice.BootstrapUtils.newServiceLocator;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Arrays.asList;
import static java.util.EnumSet.allOf;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.slf4j.LoggerFactory.getLogger;

final class StartServer {
    private static final DoubleGauge videoCacheHitRate = new DoubleGauge(builder("videoCacheHitRate_gauge").build());
    private static final DoubleGauge channelCacheHitRate = new DoubleGauge(builder("channelCacheHitRate_gauge").build());
    private static final DoubleGauge countryCountCacheHitRate = new DoubleGauge(builder("countryCountCacheHitRate_gauge").build());

    private static final DoubleGauge channelCacheLoadExceptionRate = new DoubleGauge(builder("channelCacheLoadExceptionRate_gauge").build());
    private static final DoubleGauge videoCacheLoadExceptionRate = new DoubleGauge(builder("videoCacheLoadExceptionRate_gauge").build());
    private static final DoubleGauge countryCountCacheLoadExceptionRate = new DoubleGauge(builder("countryCountCacheLoadExceptionRate_gauge").build());

    private static final LongGauge videoCacheEvictionCount = new LongGauge(builder("videoCacheEvictionCount_gauge").build());
    private static final LongGauge channelCacheEvictionCount = new LongGauge(builder("channelCacheEvictionCount_gauge").build());
    private static final LongGauge countryCountCacheEvictionCount = new LongGauge(builder("countryCountCacheEvictionCount_gauge").build());


    private static Logger LOGGER = getLogger(StartServer.class);

    static {
        getInstance().register(videoCacheHitRate);
        getInstance().register(channelCacheHitRate);
        getInstance().register(channelCacheLoadExceptionRate);
        getInstance().register(videoCacheLoadExceptionRate);
        getInstance().register(videoCacheEvictionCount);
        getInstance().register(channelCacheEvictionCount);
        getInstance().register(countryCountCacheEvictionCount);
        getInstance().register(countryCountCacheLoadExceptionRate);
        getInstance().register(countryCountCacheHitRate);
    }

    private StartServer() {
    }

    public static void main(String[] args) throws Exception {

        setProperty(ENABLE_JMX, "true");
        setProperty("archaius.configurationSource.additionalUrls", "file:///etc/de.conf");
        String env = getProperty("env");
        if (!isBlank(env)) {
            setProperty("archaius.deployment.environment", env);
        }
        loadCascadedPropertiesFromResources("application");
        final DynamicStringProperty appName =
                DynamicPropertyFactory.getInstance().getStringProperty("appname", "de");

        LOGGER.info("Application: " + appName.get());

        if (appName.get().equalsIgnoreCase("forecast")) {
            //warm up cache
            getCountryCategoryCountCache();
            getCountryDeviceCountCache();
            getCountryEventCountCache();
            getCountryFormatCountCache();
            getCountryLangCountCache();
        }
        HystrixPlugins.getInstance().registerMetricsPublisher(HystrixServoMetricsPublisher.getInstance());

        ServiceLocator locator = newServiceLocator();
        newInjector(locator, asList(new AppServletModule()));
        install(locator);

        ResourceConfig config = new ResourceConfig();
        config.register(JacksonFeature.class);
        config.register(MultiPartFeature.class);
        config.register(AppServlet.class);
        config.register(DeExceptionMapper.class);
        config.register(JSONParseExceptionMapper.class);

        ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor();


        scheduledExecutorService.schedule(() -> {
            channelCacheHitRate.set(getChannelVideosCache().stats().hitRate());
            videoCacheHitRate.set(getOrganicVideosCache().stats().hitRate());
            countryCountCacheHitRate.set(getPerCountryCountCache().stats().hitRate());

            channelCacheLoadExceptionRate.set(getChannelVideosCache().stats().loadExceptionRate());
            videoCacheLoadExceptionRate.set(getOrganicVideosCache().stats().loadExceptionRate());
            countryCountCacheLoadExceptionRate.set(getPerCountryCountCache().stats().loadExceptionRate());

            videoCacheEvictionCount.set(getOrganicVideosCache().stats().evictionCount());
            channelCacheEvictionCount.set(getChannelVideosCache().stats().evictionCount());
            countryCountCacheEvictionCount.set(getPerCountryCountCache().stats().evictionCount());

        }, 30, SECONDS);

        ServletContainer servletContainer = new ServletContainer(config);
        ServletHolder sh = new ServletHolder(servletContainer);

        // Setup Threadpool
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(100);
        threadPool.setMinThreads(10);
        threadPool.setName("de_pool");


        Server server = new Server(threadPool);
        ServletContextHandler context = new ServletContextHandler(server, "/");
        context.addServlet(DefaultServlet.class, "/");

        FilterHolder filterHolder = new FilterHolder(GuiceFilter.class);
        context.addFilter(filterHolder, "/*",
                allOf(DispatcherType.class));

        context.addServlet(sh, "/*");
        server.setHandler(context);

        // Extra options
        server.setDumpAfterStart(false);
        server.setDumpBeforeStop(false);
        server.setStopAtShutdown(true);

        // jmx
        MBeanContainer mbContainer = new MBeanContainer(getPlatformMBeanServer());
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
        http.setPort(dePort.get());
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

        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Server shutting down...");
                    scheduledExecutorService.shutdownNow();
                    server.stop();
                } catch (Exception e) {
                    LOGGER.error("Can not stop the Jetty server", e);
                }
            }
        });

        try {
            server.start();
            server.join();
        } catch (Exception err) {
            LOGGER.error("Could not start Jetty server", err);
            throw new IOException(err);
        }
    }

}
