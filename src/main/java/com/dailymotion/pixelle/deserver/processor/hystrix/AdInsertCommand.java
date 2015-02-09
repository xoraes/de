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


/**
 * Created by n.dhupia on 12/1/14.
 */
public class AdInsertCommand extends HystrixCommand<Void> {

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adinsert.semaphoreCount", 2);

    private static Logger logger = LoggerFactory.getLogger(AdInsertCommand.class);
    private AdUnit unit;
    private DEProcessor processor;

    public AdInsertCommand(DEProcessor processor, AdUnit unit) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.unit = unit;
        this.processor = processor;

    }

    @Override
    protected Void run() throws Exception {
        processor.insertAdUnit(unit);
        return null;
    }

    @Override
    protected Void getFallback() throws DeException {
        throw new DeException(new Throwable("Error inserting adunit"), 500);
    }

}
