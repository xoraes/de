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

public class VideoInsertCommand extends HystrixCommand<Boolean> {

    private static DynamicIntProperty semaphoreCount =
            DynamicPropertyFactory.getInstance().getIntProperty("videoinsert.semaphoreCount", 2);

    private static Logger logger = LoggerFactory.getLogger(VideoInsertCommand.class);
    private Video video;
    private DEProcessor processor;

    public VideoInsertCommand(DEProcessor processor, Video video) {

        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoInsert"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
                        .withExecutionIsolationSemaphoreMaxConcurrentRequests(semaphoreCount.get())));

        this.video = video;
        this.processor = processor;

    }

    @Override
    protected Boolean run() throws Exception {
        Boolean isCreated = processor.insertVideo(video);
        return isCreated;
    }

    @Override
    protected Boolean getFallback() {
        throw new DeException(new Throwable("Error inserting video"), 500);
    }

}
