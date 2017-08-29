import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * Created by guangfuhe on 2017/8/27.
 */
public class PathChildrenCacheRebuildTest {
    public static Logger logger = LoggerFactory.getLogger(PathChildrenCacheRebuildTest.class);

    public static void main(String[] args) {
        String root = "/win/test";
        String nodePath = root + "/rebuild";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(3000)
                .connectionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(nodePath, "xxxxxx".getBytes());
            PathChildrenCache cache = new PathChildrenCache(client, root, true);
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                        case INITIALIZED:
                            logger.info(" node initialized!");
                            break;
                        case CHILD_ADDED:
                            logger.info(" node added! " +
                                    "node.path=" + new String(event.getData().getPath()) + ", " +
                                    "node.date=" + new String(event.getData().getData()) + ", " +
                                    "node.stat" + event.getData().getStat());
                            break;

                        case CHILD_REMOVED:
                            logger.warn(" node removed! create node by");
                            break;
                        case CHILD_UPDATED:
                            logger.info(" node updated! " +
                                    "node.path=" + new String(event.getData().getPath()) + ", " +
                                    "node.date=" + new String(event.getData().getData()) + ", " +
                                    "node.stat" + event.getData().getStat());
                            countDownLatch.countDown();
                            break;
                        case CONNECTION_LOST:
                        case CONNECTION_SUSPENDED:
                            logger.info(" node connection error!, auto close zookeeper client");
                            client.close();
                            break;
                        case CONNECTION_RECONNECTED:
                            logger.info(" node reconnected! " +
                                    "node.path=" + new String(event.getData().getPath()) + ", " +
                                    "node.date=" + new String(event.getData().getData()) + ", " +
                                    "node.stat" + event.getData().getStat());
                            break;
                    }
                }
            });


            Collection<CuratorTransactionResult> results = client.inTransaction()
                    .check().forPath("/")
                    .and()
                    .create().withMode(CreateMode.EPHEMERAL).forPath("/hgf", "test".getBytes())
                    .and()
                    .commit();

            for (CuratorTransactionResult result : results) {
                System.out.println(result.getType() + "-" + result.getForPath() + "-" + result.getResultPath() + "-" + result.getResultStat());
            }

            System.out.println("ok");

//            Stat stat = new Stat();
//            System.out.println(new String(client.getData().storingStatIn(stat).forPath(nodePath)));
//            System.out.println("stat:" + stat.toString());
//            System.out.println("version:" + stat.getVersion());
//
//            Thread.sleep(10000);
//            client.setData().forPath(nodePath, "yyyyyyyy".getBytes());
//
//            stat = new Stat();
//            System.out.println(new String(client.getData().storingStatIn(stat).forPath(nodePath)));
//            System.out.println("stat:" + stat.toString());
//            System.out.println("version:" + stat.getVersion());
//
//
//            countDownLatch.await();
//            cache.rebuild();
//            Thread.sleep(10000);
//
//            client.setData().forPath(nodePath, "zzzzzzzz".getBytes());
//            stat = new Stat();
//            System.out.println(new String(client.getData().storingStatIn(stat).forPath(nodePath)));
//            System.out.println("stat:" + stat.toString());
//            System.out.println("version:" + stat.getVersion());

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
