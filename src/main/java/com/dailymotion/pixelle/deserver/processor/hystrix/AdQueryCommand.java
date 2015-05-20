package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.AdUnitProcessor;
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
public class AdQueryCommand extends HystrixCommand<List<AdUnitResponse>> {
    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adquery.semaphore.count", 100);

    private static Logger logger = LoggerFactory.getLogger(AdQueryCommand.class);
    private final SearchQueryRequest sq;
    private final int positions;


    public AdQueryCommand(SearchQueryRequest sq, int pos) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.sq = sq;
        this.positions = pos;
    }

    @Override
    protected List<AdUnitResponse> run() throws Exception {
        return AdUnitProcessor.recommend(sq, positions);
    }
}
