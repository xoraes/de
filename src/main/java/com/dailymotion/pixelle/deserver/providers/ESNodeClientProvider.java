package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Provider;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by n.dhupia on 10/29/14.
 */
public class ESNodeClientProvider implements Provider<Client> {

    public Client get() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.getNode())
                .put("path.data", DeHelper.getDataDir())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);

        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(DeHelper.getCluster())
                .client(false)
                .local(true)
                .node()
                .client();

        //createIndex accepts multiple types name delimited by ,
        //creates index only if it does not exist
        ESIndexTypeFactory.createIndex(client, DeHelper.getIndex(), elasticsearchSettings.build(), DeHelper.getAdUnitsType(), DeHelper.getOrganicVideoType());
        return client;
    }
}
