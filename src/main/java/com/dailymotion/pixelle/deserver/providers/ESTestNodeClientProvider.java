package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Provider;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by n.dhupia on 10/29/14.
 */
public class ESTestNodeClientProvider implements Provider<Client> {
    private static Logger logger = LoggerFactory.getLogger(ESTestNodeClientProvider.class);

    public Client get() {
        boolean ack;
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.getNode())
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);


        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(DeHelper.getCluster())
                .data(true)
                .client(false)
                .local(true)
                .node()
                .client();

        //createIndex accepts multiple types name delimited by ,
        ESIndexTypeFactory.createIndex(client, DeHelper.getIndex(), elasticsearchSettings.build(), DeHelper.getAdUnitsType());
        return client;
    }
}
