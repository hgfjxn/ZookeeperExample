package win.hgfdodo.example.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * Created by guangfuhe on 2017/9/2.
 */
public class NodeWatcherExample {
    private CuratorFramework client;
    private String nodePath;

    public NodeWatcherExample(CuratorFramework client, String nodePath) {
        this.client = client;
        this.nodePath = nodePath;
    }

    public void init() {
        this.client.start();
        try {
            Stat stat = this.client.checkExists().forPath(nodePath);
            if (null == stat) {
                this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(nodePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        watch();
    }

    private void watch() {
        NodeCache nodeCache = new NodeCache(client, nodePath);
        try {
            nodeCache.start();
            nodeCache.getListenable().addListener(new NodeCacheListenerAdapter(client, nodePath));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(6000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        String testNode = "/test/node";
        NodeWatcherExample nodeWatcherExample = new NodeWatcherExample(client, testNode);
        nodeWatcherExample.init();

        client.setData().forPath(testNode, "set1".getBytes());
        Thread.sleep(10000);
        client.setData().forPath(testNode, "set2".getBytes());
        client.setData().forPath(testNode, "set3".getBytes());

        Thread.sleep(10000);
        client.setData().forPath(testNode, "set4".getBytes());

        Thread.sleep(10000);
        Stat stat = new Stat();
        byte[] data = client.getData().storingStatIn(stat).forPath(testNode);
        System.out.println("data " + new String(data) + ", stat " + stat);
    }
}
