package com.dailymotion.pixelle.de.processor;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/4/14.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DeException extends Exception {
    private static final Logger LOGGER = getLogger(DeException.class);

    private int status;
    private String msg;

    public DeException(int status, String msg) {
        super(msg);
        this.status = status;
        this.msg = msg;
        LOGGER.error(msg);
    }

    public DeException(Throwable throwable, int status) {
        super(throwable);
        this.msg = throwable.getMessage();
        this.status = status;
        LOGGER.error(throwable.getMessage());
    }
}
