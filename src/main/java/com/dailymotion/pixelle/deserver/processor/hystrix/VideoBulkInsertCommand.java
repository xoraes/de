package com.dailymotion.pixelle.deserver.processor.hystrix;

/**
 * Created by n.dhupia on 2/4/15.
 */

import com.dailymotion.pixelle.deserver.model.Video;
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
 * Created by n.dhupia on 2/4/15.
 */
public class VideoBulkInsertCommand extends HystrixCommand<Void> {
    private static Logger logger = LoggerFactory.getLogger(VideoBulkInsertCommand.class);

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("videoBulkInsert.semaphoreCount", 10);

    private List<Video> videos;
    private DEProcessor processor;

    public VideoBulkInsertCommand(DEProcessor deProcessor, List<Video> videos) {
        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.videos = videos;
        this.processor = deProcessor;
    }

    @Override
    protected Void run() throws DeException {
        processor.insertVideoInBulk(videos);
        return null;
    }

    @Override
    protected Void getFallback() throws DeException {
        throw new DeException(new Throwable("Error Inserting video"), 500);
    }
}
