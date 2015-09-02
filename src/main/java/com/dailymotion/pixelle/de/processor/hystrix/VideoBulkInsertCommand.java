package com.dailymotion.pixelle.de.processor.hystrix;

/**
 * Created by n.dhupia on 2/4/15.
 */

import com.dailymotion.pixelle.de.model.Video;
import com.netflix.config.DynamicIntProperty;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandProperties;
import org.slf4j.Logger;

import java.util.List;

import static com.dailymotion.pixelle.de.processor.DEProcessor.insertVideoInBulk;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.hystrix.HystrixCommand.Setter.withGroupKey;
import static com.netflix.hystrix.HystrixCommandGroupKey.Factory.asKey;
import static com.netflix.hystrix.HystrixCommandKey.Factory;
import static com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 2/4/15.
 */
public class VideoBulkInsertCommand extends HystrixCommand<Void> {
    private static final DynamicIntProperty semaphoreCount =
            getInstance().getIntProperty("videobulkbinsert.semaphore.count", 10);
    private static final DynamicIntProperty timeoutMillis =
            getInstance().getIntProperty("videobulkbinsert.timeout.milliseconds", 60000);

    private static Logger logger = getLogger(VideoBulkInsertCommand.class);
    private final List<Video> videos;

    public VideoBulkInsertCommand(List<Video> videos) {
        super(withGroupKey(asKey("DecisioningEngine"))
                .andCommandKey(Factory.asKey("VideoBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeoutMillis.get())
                        .withExecutionIsolationStrategy(SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.videos = videos;
    }

    @Override
    protected Void run() throws Exception {
        insertVideoInBulk(videos);
        return null;
    }
}
