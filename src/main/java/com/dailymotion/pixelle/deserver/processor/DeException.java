package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.logger.InjectLogger;
import org.slf4j.Logger;

import javax.ws.rs.WebApplicationException;

/**
 * Created by n.dhupia on 11/4/14.
 */
public class DeException extends WebApplicationException {
    @InjectLogger
    static Logger logger;

    public DeException(Throwable throwable, int statusCode) {
        super(throwable, statusCode);
    }
}
