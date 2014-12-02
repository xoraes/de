package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by n.dhupia on 12/1/14.
 */
public class AdQueryCommand extends HystrixCommand<ItemsResponse> {

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("adquery.semaphoreCount", 10);

    private static Logger logger = LoggerFactory.getLogger(AdQueryCommand.class);
    private SearchQueryRequest sq;
    private int position;
    private String allowedTypes;
    private DEProcessor processor;

    public AdQueryCommand(DEProcessor processor, SearchQueryRequest sq, int pos, String allowedTypes) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("AdQuery"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.sq = sq;
        this.position = pos;
        this.allowedTypes = allowedTypes;
        this.processor = processor;

    }

    @Override
    protected ItemsResponse run() throws Exception {
        position = position <= 0 ? 1 : position;
        //default allowed type is promoted
        allowedTypes = allowedTypes == null || allowedTypes.trim() == "" ? "promoted" : allowedTypes;
        String[] at = StringUtils.split(allowedTypes);
        ItemsResponse i = processor.recommend(sq, position, at);
        if (i == null) {
            logger.info("No ads returned =======> " + sq.toString());
        } else {
            logger.info("Success =======> " + i.toString());
        }
        return i;
    }

    @Override
    protected ItemsResponse getFallback() {
        //in future we can do return a fallback adunit here or do something smarter here
        throw new DeException(new Throwable("Error querying adunit"), 500);
    }
}
