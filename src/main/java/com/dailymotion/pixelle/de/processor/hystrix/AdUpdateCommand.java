package com.dailymotion.pixelle.de.processor.hystrix;


import com.dailymotion.pixelle.de.model.AdUnit;
import com.dailymotion.pixelle.de.processor.AdUnitProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dailymotion.pixelle.de.processor.AdUnitProcessor.updateAdUnit;
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
public class AdUpdateCommand extends HystrixCommand<Void> {

    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("adupdate.semaphore.count", 10);
    private static Logger LOGGER = getLogger(AdUpdateCommand.class);
    private final AdUnit unit;

    public AdUpdateCommand(AdUnit unit) {

        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdUpdate"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.unit = unit;
    }

    @Override
    protected Void run() throws Exception {
        updateAdUnit(unit, false);
        return null;
    }
}
