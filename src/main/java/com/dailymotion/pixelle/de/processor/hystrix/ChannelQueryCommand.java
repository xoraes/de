package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.ChannelProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.dailymotion.pixelle.de.processor.ChannelProcessor.recommendChannel;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class ChannelQueryCommand extends HystrixCommand<List<VideoResponse>> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("channelquery.semaphore.count", 100);
    private static final DynamicIntProperty timeout = getInstance().getIntProperty("hystrix.channel.query.timeout", 5000);
    private static Logger LOGGER = getLogger(ChannelQueryCommand.class);
    private final SearchQueryRequest sq;
    private final int positions;

    public ChannelQueryCommand(SearchQueryRequest sq, Integer positions) {

        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ChannelQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        this.sq = sq;
        this.positions = positions;
    }

    @Override
    protected List<VideoResponse> run() throws Exception {
        List<VideoResponse> vrl = null;
        try {
            vrl = recommendChannel(sq, positions);
        } catch (Exception e) {
            if (e.getCause() instanceof HystrixBadRequestException) {
                throw new HystrixBadRequestException(e.getMessage(), e.getCause());
            }
            throw e;
        }
        return vrl;
    }
}
