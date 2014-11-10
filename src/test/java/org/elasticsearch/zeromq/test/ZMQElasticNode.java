package org.elasticsearch.zeromq.test;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ZMQElasticNode {

    private static Node node = null;

    public static void startNode() {
        NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("es.config", "elasticsearch.yml")).node();
    }

    public static Node getNode() {
        return node;
    }

    public static void stopNode() {
        node.close();
        node = null;
    }

}
