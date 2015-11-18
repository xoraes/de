package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.common.services.DMApiErrorDecoder;
import com.dailymotion.pixelle.common.services.DMApiService;
import com.dailymotion.pixelle.de.model.DMApiResponse;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.processor.DeException;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicStringProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.slf4j.Logger;

import java.util.List;

import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandKey.Factory;
import static feign.Feign.builder;
import static feign.Retryer.Default;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.isClientError;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class DMApiQueryCommand extends HystrixCommand<List<Video>> {
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

    private static Logger logger = getLogger(DMApiQueryCommand.class);
    private String channels;
    private String playlist;
    private String sortOrder;


    public DMApiQueryCommand(String channels, String playlist, String sortOrder) {
        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(Factory.asKey("DMChannelQuery"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("DMChannelQueryPool"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        this.channels = channels;
        this.playlist = playlist;
        this.sortOrder = sortOrder;
    }

    @Override
    protected List<Video> run() throws DeException {

        boolean isChannels = isNotBlank(channels);
        boolean isPlayList = isNotBlank(playlist);

        if (!isChannels && !isPlayList) {
            throw new DeException(BAD_REQUEST_400, "No channels or playlist were provided");
        }
        List<Video> cvs = null;
        try {
            DMApiResponse re = null;
            if (isChannels) {
                re = dmApi.getChannelVideos(channels, sortOrder);
            } else {
                re = dmApi.getPlaylistVideos(playlist, sortOrder);
            }
            if (re != null) {
                cvs = re.getList();
            }
        } catch (DeException e) {
            if (isClientError(e.getStatus())) {
                throw new HystrixBadRequestException(e.getMessage(), e);
            }
            throw e;
        }
        return cvs;
    }
}
