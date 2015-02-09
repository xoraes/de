package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
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
 * Created by n.dhupia on 2/6/15.
 */
public class AdUnitBulkInsertCommand extends HystrixCommand<Void> {
    private static Logger logger = LoggerFactory.getLogger(AdUnitBulkInsertCommand.class);

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adUnitBulkInsert.semaphoreCount", 10);

    private List<AdUnit> adUnits;
    private DEProcessor processor;

    public AdUnitBulkInsertCommand(DEProcessor deProcessor, List<AdUnit> adUnits) {
        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdUnitBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.adUnits = adUnits;
        this.processor = deProcessor;
    }

    @Override
    protected Void run() throws DeException {
        processor.insertAdUnitsInBulk(adUnits);
        return null;
    }

    @Override
    protected Void getFallback() throws DeException {
        throw new DeException(new Throwable("Error Inserting adunits"), 500);
    }
}
