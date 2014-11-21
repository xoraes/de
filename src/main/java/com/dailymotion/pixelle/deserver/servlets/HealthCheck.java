package com.dailymotion.pixelle.deserver.servlets;


import com.dailymotion.pixelle.deserver.logger.InjectLogger;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;


/**
 * Created by n.dhupia on 10/31/14.
 */

public class HealthCheck {
    @InjectLogger
    private Logger logger;
    private ClusterHealthResponse clusterHealthResponse;
    private Client client;

    @Inject
    public HealthCheck(Client client) {
        this.client = client;
    }


    public ClusterHealthResponse getHealthCheck() {
        ActionFuture<ClusterHealthResponse> resp = client.admin().cluster().health(new ClusterHealthRequest(DeHelper.getIndex()));
        return resp.actionGet();
    }
}
