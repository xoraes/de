package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.VideoProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.dailymotion.pixelle.de.processor.VideoProcessor.recommendUsingCache;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/1/14.
 */
public class VideoQueryCommand extends HystrixCommand<List<VideoResponse>> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("videoquery.semaphore.count", 100);

    private static Logger LOGGER = getLogger(AdQueryCommand.class);
    private final SearchQueryRequest sq;
    private final Integer positions;

    public VideoQueryCommand(SearchQueryRequest sq, Integer positions) {
        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.sq = sq;
        this.positions = positions;
    }

    @Override
    protected List<VideoResponse> run() throws Exception {
        return recommendUsingCache(sq, positions);
    }
}
