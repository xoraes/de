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

import java.util.List;

/**
 * Created by n.dhupia on 2/6/15.
 */
public class AdUnitBulkInsertCommand extends HystrixCommand<Void> {
    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adunitbulkinsert.semaphore.count", 10);
    private static final DynamicIntProperty timeoutMillis =
            DynamicPropertyFactory.getInstance().getIntProperty("adunitbulkbinsert.timeout.milliseconds", 60000);

    private static Logger logger = LoggerFactory.getLogger(AdUnitBulkInsertCommand.class);
    private final List<AdUnit> adUnits;

    public AdUnitBulkInsertCommand(List<AdUnit> adUnits) {
        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdUnitBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeoutMillis.get())
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.adUnits = adUnits;
    }

    @Override
    protected Void run() throws Exception {
        AdUnitProcessor.insertAdUnitsInBulk(adUnits);
        return null;
    }
}
