package com.dailymotion.pixelle.deserver.processor.hystrix;


import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
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
public class VideoUpdateCommand extends HystrixCommand<Void> {

    private static final DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("videoupdate.semaphore.count", 10);
    private static Logger logger = LoggerFactory.getLogger(VideoUpdateCommand.class);
    private final Video video;

    public VideoUpdateCommand(Video video) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoUpdate"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.video = video;
    }

    @Override
    protected Void run() throws Exception {
        DEProcessor.updateVideo(video);
        return null;
    }
}
