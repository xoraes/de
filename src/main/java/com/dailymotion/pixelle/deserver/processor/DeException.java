package com.dailymotion.pixelle.deserver.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;

/**
 * Created by n.dhupia on 11/4/14.
 */
public class DeException extends WebApplicationException {
    private static Logger logger = LoggerFactory.getLogger(DeException.class);

    public DeException(Throwable throwable, int statusCode) {
        super(throwable, statusCode);
        logger.error(String.valueOf(statusCode), throwable);
    }

    public DeException(String cause, Throwable throwable) {
        super(cause, throwable);
        logger.error(cause, throwable);
    }
}
