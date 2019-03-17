package org.liguang.raft;

import lombok.Data;
import org.liguang.raft.server.ServerNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author liguang
 */
@Data
public class Configuration {

    private ServerNode localServer = new ServerNode();
    private List<ServerNode> clusters = new ArrayList<ServerNode>();

    public Configuration() {
        init();
    }

    private void init() {
        Properties properties = new Properties();
        try {
            properties.load(Configuration.class.getResourceAsStream("/config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] clusters = properties.getProperty("clusters").split(",");
        for (String cluster : clusters) {
            String[] split = cluster.split(":");
            ServerNode serverNode = new ServerNode();
            serverNode.setHost(split[split.length - 2]);
            serverNode.setPort(Integer.valueOf(split[split.length - 1]));

            this.clusters.add(serverNode);
        }

        localServer.setPort(Integer.valueOf(properties.getProperty("serverPort")));
        localServer.setHost("localhost");
        localServer.setId(Integer.valueOf(properties.getProperty("serverId")));
        localServer.setPeers(this.clusters);

    }

}
