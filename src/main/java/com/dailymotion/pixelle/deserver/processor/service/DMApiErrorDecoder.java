package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.processor.DeException;
import feign.Response;
import feign.codec.ErrorDecoder;
import org.eclipse.jetty.http.HttpStatus;

/**
 * Created by n.dhupia on 5/13/15.
 */
public class DMApiErrorDecoder implements ErrorDecoder {

    @Override
    public Exception decode(String methodKey, Response response) {
        if (HttpStatus.isClientError(response.status())) {
            return new DeException(response.status(), response.reason());
        }
        return new DeException(response.status(), response.reason());
    }
}