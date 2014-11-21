package com.dailymotion.pixelle.deserver.servlets;


import com.dailymotion.pixelle.deserver.logger.InjectLogger;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;


/**
 * Created by n.dhupia on 10/31/14.
 */

public class HealthCheck {
    @InjectLogger
    private Logger logger;
    private ClusterStatsResponse clusterStatsResponse;
    private Client client;

    @Inject
    public HealthCheck(Client client) {
        this.client = client;
    }

    public ClusterStatsResponse getClusterStatsResponse() {
        return clusterStatsResponse;
    }

    public void setClusterStatsResponse(ClusterStatsResponse clusterStatsResponse) {
        this.clusterStatsResponse = clusterStatsResponse;
    }

    public HealthCheck getHealthCheck() {
        ActionFuture<ClusterStatsResponse> resp = client.admin().cluster()
                .clusterStats(new ClusterStatsRequest(DeHelper.getNode()));

        setClusterStatsResponse(resp.actionGet());
        return this;
    }

}
