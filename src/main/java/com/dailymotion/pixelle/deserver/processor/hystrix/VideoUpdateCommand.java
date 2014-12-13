package com.dailymotion.pixelle.deserver.processor.hystrix;


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


/**
 * Created by n.dhupia on 12/1/14.
 */
public class VideoUpdateCommand extends HystrixCommand<Boolean> {

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("videoupdate.semaphoreCount", 10);
    private static Logger logger = LoggerFactory.getLogger(VideoUpdateCommand.class);
    private Video video;
    private DEProcessor processor;

    public VideoUpdateCommand(DEProcessor processor, Video video) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoUpdate"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));
        this.video = video;
        this.processor = processor;
    }

    @Override
    protected Boolean run() throws Exception {
        Boolean isUpdated = processor.updateVideo(video);
        return isUpdated;
    }

    @Override
    protected Boolean getFallback() {
        throw new DeException(new Throwable("Error updating video"), 500);
    }
}
