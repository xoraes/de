package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.ChannelProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class ChannelQueryCommand extends HystrixCommand<List<VideoResponse>> {
    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("channelquery.semaphore.count", 100);
    private static final DynamicIntProperty timeout = DynamicPropertyFactory.getInstance().getIntProperty("hystrix.channel.query.timeout", 5000);
    private static Logger logger = LoggerFactory.getLogger(ChannelQueryCommand.class);
    private final SearchQueryRequest sq;
    private final int positions;

    public ChannelQueryCommand(SearchQueryRequest sq, Integer positions) {

        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ChannelQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        this.sq = sq;
        this.positions = positions;
    }

    @Override
    protected List<VideoResponse> run() throws Exception {
        return ChannelProcessor.recommend(sq, positions);
    }
}
