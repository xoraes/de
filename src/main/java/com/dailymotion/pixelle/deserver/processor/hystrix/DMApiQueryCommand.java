package com.dailymotion.pixelle.deserver.processor.hystrix;

import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.processor.ChannelProcessor;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 3/2/15.
 */
public class DMApiQueryCommand extends HystrixCommand<List<Video>> {
    private static final DynamicIntProperty timeout = DynamicPropertyFactory.getInstance().getIntProperty("hystrix.dm.api.timeout", 5000);
    private static Logger logger = LoggerFactory.getLogger(DMApiQueryCommand.class);
    private final String channelId;

    public DMApiQueryCommand(String channel) {

        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("DecisioningEngine"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("DMChannelQuery"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("DMChannelQueryPool"))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationThreadTimeoutInMilliseconds(timeout.get())));

        channelId = channel;
    }

    @Override
    protected List<Video> run() throws Exception {
        return ChannelProcessor.getVideosFromDM(channelId);
    }
}
