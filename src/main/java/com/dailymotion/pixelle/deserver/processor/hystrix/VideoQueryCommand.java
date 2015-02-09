package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.VideoProcessor;
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
 * Created by n.dhupia on 12/1/14.
 */
public class VideoQueryCommand extends HystrixCommand<List<VideoResponse>> {
    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("videoquery.semaphoreCount", 10);

    private static Logger logger = LoggerFactory.getLogger(AdQueryCommand.class);
    private SearchQueryRequest sq;
    private VideoProcessor processor;
    private Integer positions;

    public VideoQueryCommand(VideoProcessor processor, SearchQueryRequest sq, Integer positions) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.sq = sq;
        this.processor = processor;
        this.positions = positions;
    }

    @Override
    protected List<VideoResponse> run() throws Exception {
        return processor.recommend(sq, positions, null);
    }

    @Override
    protected List<VideoResponse> getFallback() throws DeException {
        //in future we can do return a fallback adunit here or do something smarter here
        throw new DeException(new Throwable("Error querying video"), 500);
    }
}
