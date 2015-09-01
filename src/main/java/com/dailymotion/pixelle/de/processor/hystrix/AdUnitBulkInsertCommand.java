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

import java.util.List;

import static com.dailymotion.pixelle.de.processor.AdUnitProcessor.insertAdUnitsInBulk;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 2/6/15.
 */
public class AdUnitBulkInsertCommand extends HystrixCommand<Void> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("adunitbulkinsert.semaphore.count", 10);
    private static final DynamicIntProperty timeoutMillis =
            getInstance().getIntProperty("adunitbulkbinsert.timeout.milliseconds", 60000);

    private static Logger LOGGER = getLogger(AdUnitBulkInsertCommand.class);
    private final List<AdUnit> adUnits;

    public AdUnitBulkInsertCommand(List<AdUnit> adUnits) {
        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdUnitBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeoutMillis.get())
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.adUnits = adUnits;
    }

    @Override
    protected Void run() throws Exception {
        insertAdUnitsInBulk(adUnits);
        return null;
    }
}
