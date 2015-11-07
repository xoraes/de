package com.dailymotion.pixelle.common.services;

import com.dailymotion.pixelle.de.processor.DeException;
import feign.Response;
import feign.codec.ErrorDecoder;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by n.dhupia on 5/13/15.
 */
public class DMApiErrorDecoder implements ErrorDecoder {
    private static final Logger logger = getLogger(DMApiErrorDecoder.class);
    @Override
    public Exception decode(String methodKey, Response response) {
        logErrorResponse(response);
        return new DeException(response.status(), response.reason());
    }

    private void logErrorResponse(Response response) {
        Map<String, Collection<String>> headers = response.headers();
        for (String key : headers.keySet()) {
            StringBuilder sb = new StringBuilder();
            Collection<String> values = headers.get(key);
            Iterator<String> valuesIt = values.iterator();
            while (valuesIt.hasNext()) {
                sb.append(valuesIt.next());
            }
            logger.error(key + " : " + sb.toString());
        }
    }
}
