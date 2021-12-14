package com.zk.distribute;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributeServer {
    private static String connectString = "hadoopcm2:2181,hadoopcm3:2181,hadoopcm4:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private static final String parentNode = "/servers";
    private static final Logger logger = LoggerFactory.getLogger(DistributeServer.class);

    public static void main(String[] args) throws Exception {
        DistributeServer distributeServer = new DistributeServer();
        distributeServer.connect();

        distributeServer.registerServer(args[0]);

        distributeServer.business(args[0]);
    }

    private void business(String hostname) throws Exception {
        System.out.println(hostname + " is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    private void registerServer(String hostname) throws Exception {
        String create = zkClient.create(parentNode + "/server" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info(hostname + " is online " + create);
    }

    private void connect() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}
