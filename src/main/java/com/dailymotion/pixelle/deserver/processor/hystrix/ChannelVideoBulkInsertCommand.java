package com.dailymotion.pixelle.deserver.processor.hystrix;

/**
 * Created by n.dhupia on 2/4/15.
 */

import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.processor.ChannelProcessor;
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
public class ChannelVideoBulkInsertCommand extends HystrixCommand<Void> {
    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("channel.videobulkbinsert.semaphore.count", 10);
    private static final DynamicIntProperty timeoutMillis =
            DynamicPropertyFactory.getInstance().getIntProperty("channel.videobulkbinsert.timeout.milliseconds", 60000);
    private static Logger logger = LoggerFactory.getLogger(ChannelVideoBulkInsertCommand.class);
    private final List<Video> videos;


    public ChannelVideoBulkInsertCommand(List<Video> videos) {
        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ChannelVideoBulkInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionTimeoutInMilliseconds(timeoutMillis.get())
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.videos = videos;

    }

    @Override
    protected Void run() throws Exception {
        ChannelProcessor.insertChannelVideoInBulk(videos);
        return null;
    }
}
