package com.dailymotion.pixelle.forecast.processor;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/4/14.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ForecastException extends Exception {
    private static final Logger logger = getLogger(ForecastException.class);

    private int status;
    private String msg;

    public ForecastException(int status, String msg) {
        super(msg);
        this.status = status;
        this.msg = msg;
        logger.error(msg);
    }

    public ForecastException(Throwable throwable, int status) {
        super(throwable);
        this.msg = throwable.getMessage();
        this.status = status;
        logger.error(throwable.getMessage());
    }
}
