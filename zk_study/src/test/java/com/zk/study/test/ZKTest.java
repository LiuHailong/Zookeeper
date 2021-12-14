package com.zk.study.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZKTest {

    private static String connectString = "hadoopcm2:2181,hadoopcm3:2181,hadoopcm4:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private static final Logger logger = LoggerFactory.getLogger(ZKTest.class);

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数
//                System.out.println(event.getType());
                logger.info("type:" + Event.EventType.fromInt(event.getType().getIntValue()).name() + "" +
                        " path:" + event.getPath() + "" +
                        " state:" + Event.KeeperState.fromInt(event.getState().getIntValue()).name());

                try {
                    zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void testCreate() throws Exception {
        zkClient.create("/sanguo/wangbadan", "王八蛋".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testListChildren() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    // 判断znode是否存在
    @Test
    public void exist() throws Exception {

        Stat stat = zkClient.exists("/eclipse", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }
}
