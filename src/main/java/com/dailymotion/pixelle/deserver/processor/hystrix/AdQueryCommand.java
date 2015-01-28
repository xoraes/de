package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.AdUnitProcessor;
import com.dailymotion.pixelle.deserver.processor.DeException;
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
    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adquery.semaphoreCount", 10);

    private static Logger logger = LoggerFactory.getLogger(AdQueryCommand.class);
    private SearchQueryRequest sq;
    private int positions;
    private AdUnitProcessor processor;


    public AdQueryCommand(AdUnitProcessor processor, SearchQueryRequest sq, int pos) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.sq = sq;
        this.positions = pos;
        this.processor = processor;

    }

    @Override
    protected List<AdUnitResponse> run() throws Exception {
        return processor.recommend(sq, positions);
    }

    @Override
    protected List<AdUnitResponse> getFallback() {
        //in future we can do return a fallback adunit here or do something smarter here
        throw new DeException(new Throwable("Error querying adunit"), 500);
    }
}