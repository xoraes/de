package com.dailymotion.pixelle.de.processor.hystrix;

import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.processor.DeException;
import com.netflix.config.DynamicIntProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.slf4j.Logger;

import static com.dailymotion.pixelle.de.processor.DEProcessor.recommend;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.eclipse.jetty.http.HttpStatus.isClientError;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/11/14.
 */
public class QueryCommand extends HystrixCommand<ItemsResponse> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("query.semaphore.count", 100);
    private static final DynamicIntProperty timeout = getInstance().getIntProperty("hystrix.query.timeout", 5000);
    private static Logger LOGGER = getLogger(QueryCommand.class);
    private final SearchQueryRequest sq;
    private final String allowedTypes;
    private final Integer positions;

    public QueryCommand(SearchQueryRequest sq, Integer positions, String allowedTypes) {

        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("Query"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())
                        .withExecutionTimeoutInMilliseconds(timeout.get())));
        this.sq = sq;
        this.allowedTypes = allowedTypes;
        this.positions = positions;
    }

    @Override
    protected ItemsResponse run() throws Exception {
        ItemsResponse items = null;

        try {
            items = recommend(sq, positions, allowedTypes);
        } catch (DeException e) {
            if (isClientError(e.getStatus())) {
                throw new HystrixBadRequestException(e.getMsg());
            }
            throw e;
        }
        return items;
    }
}
