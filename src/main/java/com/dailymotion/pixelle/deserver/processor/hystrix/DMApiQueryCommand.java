package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.service.DMApiService;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;
import feign.Feign;
import feign.Retryer;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class DMApiQueryCommand extends HystrixCommand<ChannelVideos> {
    private static final DynamicIntProperty timeout = DynamicPropertyFactory.getInstance().getIntProperty("hystrix.dm.api.timeout", 5000);
    private static final DynamicStringProperty dmApiUrl = DynamicPropertyFactory.getInstance().getStringProperty("dm.api.url", "https://api.dailymotion.com");
    private static final DynamicLongProperty retryPeriod = DynamicPropertyFactory.getInstance().getLongProperty("dm.api.retry.period", 100);
    private static final DynamicLongProperty retryMaxPeriod = DynamicPropertyFactory.getInstance().getLongProperty("dm.api.retry.max.period", 1);
    private static final DynamicIntProperty retryMaxAttempts = DynamicPropertyFactory.getInstance().getIntProperty("dm.api.retry.max.attempts", 5);

    private static Logger logger = LoggerFactory.getLogger(DMApiQueryCommand.class);
    private final String channelId;

    public DMApiQueryCommand(String channel) {

        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("DMChannelQuery"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("DMChannelQueryPool"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        channelId = channel;
    }

    @Override
    protected ChannelVideos run() throws Exception {
        if (StringUtils.isBlank(channelId)) {
            throw new DeException(new Throwable("No channel id provided"), HttpStatus.BAD_REQUEST_400);
        }
        DMApiService dmApi = Feign.builder()
                .retryer(new Retryer.Default(retryPeriod.get(), TimeUnit.SECONDS.toMillis(retryMaxPeriod.get()), retryMaxAttempts.get()))
                .decoder(new JacksonDecoder())
                .encoder(new JacksonEncoder())
                .target(DMApiService.class, dmApiUrl.get());
        return dmApi.getVideos(channelId);
    }
}
