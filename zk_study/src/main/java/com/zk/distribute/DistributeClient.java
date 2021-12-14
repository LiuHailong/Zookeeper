package com.zk.distribute;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private static String connectString = "hadoopcm2:2181,hadoopcm3:2181,hadoopcm4:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private static final String parentNode = "/servers";
    private static final Logger logger = LoggerFactory.getLogger(DistributeClient.class);

    public static void main(String[] args) throws Exception {
        DistributeClient distributeClient = new DistributeClient();
        distributeClient.getConnect();

        distributeClient.getServerList();

        distributeClient.business();
    }

    private void business() throws Exception {
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registerServer(String hostname) throws Exception {
        String create = zkClient.create(parentNode + "/server" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info(hostname + " is online " + create);
    }

    private void getConnect() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void getServerList() throws Exception{
        List<String> children = zkClient.getChildren(parentNode, true);
        // 2存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        // 4打印服务器列表信息
        System.out.println(servers);
    }
}
