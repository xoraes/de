package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.common.services.DMApiErrorDecoder;
import com.dailymotion.pixelle.common.services.DMApiService;
import com.dailymotion.pixelle.de.model.ChannelVideos;
import com.dailymotion.pixelle.de.model.Channels;
import com.dailymotion.pixelle.de.processor.DeException;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicStringProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.slf4j.Logger;

import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static feign.Feign.builder;
import static feign.Retryer.Default;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.isClientError;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class DMApiQueryCommand extends HystrixCommand<ChannelVideos> {
    private static final DynamicIntProperty timeout = getInstance().getIntProperty("hystrix.dm.api.timeout", 5000);
    private static final DynamicStringProperty dmApiUrl = getInstance().getStringProperty("dm.api.url", "https://api.dailymotion.com");
    private static final DynamicLongProperty retryPeriod = getInstance().getLongProperty("dm.api.retry.period", 100);
    private static final DynamicLongProperty retryMaxPeriod = getInstance().getLongProperty("dm.api.retry.max.period", 1);
    private static final DynamicIntProperty retryMaxAttempts = getInstance().getIntProperty("dm.api.retry.max.attempts", 5);
    private static final DMApiService dmApi = builder()
            .retryer(new Default(retryPeriod.get(), SECONDS.toMillis(retryMaxPeriod.get()), retryMaxAttempts.get()))
            .decoder(new JacksonDecoder())
            .encoder(new JacksonEncoder())
            .errorDecoder(new DMApiErrorDecoder())
            .target(DMApiService.class, dmApiUrl.get());

    private static Logger LOGGER = getLogger(DMApiQueryCommand.class);
    private Channels channels;


    public DMApiQueryCommand(Channels channels) {
        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("DMChannelQuery"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("DMChannelQueryPool"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        this.channels = channels;
    }

    @Override
    protected ChannelVideos run() throws DeException {
        if (isBlank(channels.getChannels())) {
            throw new DeException(BAD_REQUEST_400, "No channels were provided");
        }
        ChannelVideos cvs;
        try {
            cvs = dmApi.getVideos(channels.getChannels(), channels.getSortOrder());
        } catch (DeException e) {
            if (isClientError(e.getStatus())) {
                throw new HystrixBadRequestException(e.getMessage(), e.getCause());
            }
            throw e;
        }
        return cvs;
    }
}
