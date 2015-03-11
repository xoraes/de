package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.processor.AdUnitProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by n.dhupia on 12/1/14.
 */
public class AdInsertCommand extends HystrixCommand<Void> {

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adinsert.semaphore.count", 10);

    private static Logger logger = LoggerFactory.getLogger(AdInsertCommand.class);
    private AdUnit unit;

    public AdInsertCommand(AdUnit unit) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.unit = unit;
    }

    @Override
    protected Void run() {
        AdUnitProcessor.insertAdUnit(unit);
        return null;
    }
}
