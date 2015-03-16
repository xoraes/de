package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;

/**
 * Created by n.dhupia on 12/11/14.
 */
public class QueryCommand extends HystrixCommand<ItemsResponse> {

    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("query.semaphore.count", 100);
    private final SearchQueryRequest sq;
    private final String allowedTypes;
    private final Integer positions;

    public QueryCommand(SearchQueryRequest sq, Integer positions, String allowedTypes) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("Query"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.sq = sq;
        this.allowedTypes = allowedTypes;
        this.positions = positions;
    }

    @Override
    protected ItemsResponse run() throws Exception {
        return DEProcessor.recommend(sq, positions, allowedTypes);
    }
}
