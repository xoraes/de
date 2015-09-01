package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.de.model.AdUnitResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.dailymotion.pixelle.de.processor.AdUnitProcessor.recommend;
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
public class AdQueryCommand extends HystrixCommand<List<AdUnitResponse>> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("adquery.semaphore.count", 100);

    private static Logger LOGGER = getLogger(AdQueryCommand.class);
    private final SearchQueryRequest sq;
    private final int positions;


    public AdQueryCommand(SearchQueryRequest sq, int pos) {

        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.sq = sq;
        this.positions = pos;
    }

    @Override
    protected List<AdUnitResponse> run() throws Exception {
        return recommend(sq, positions);
    }
}
