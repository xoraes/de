package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.de.model.AdUnit;
import com.netflix.config.DynamicIntProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;

import static com.dailymotion.pixelle.de.processor.AdUnitProcessor.insertAdUnit;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.slf4j.LoggerFactory.getLogger;


/**
 * Created by n.dhupia on 12/1/14.
 */
public class AdInsertCommand extends HystrixCommand<Void> {

    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("adinsert.semaphore.count", 10);

    private static Logger LOGGER = getLogger(AdInsertCommand.class);
    private final AdUnit unit;

    public AdInsertCommand(AdUnit unit) {

        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.unit = unit;
    }

    @Override
    protected Void run() throws Exception {
        insertAdUnit(unit);
        return null;
    }
}
