package win.hgfdodo.framework.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.CountDownLatch;

/**
 * Created by guangfuhe on 2017/8/27.
 * 使用说明：
 * 1. 使用项目属性和zookeeper连接配置实例化
 * 2. 使用init初始化节点和连接，开启自动切换逻辑。
 */
public class AutoSwitch {
    private static Logger logger = LoggerFactory.getLogger(AutoSwitch.class);

    private CuratorFramework client;

    /**
     * AtuoSwitch 开始工作后，阻塞主进程。直到监听到创建新节点的连接是本连接。
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 监听的对象为root节点，这样才能发现root/project节点的消失
     */
    private PathChildrenCache pathChildrenCache;

    /**
     * project name.
     * It will create as a node name.
     */
    private String project;

    private String projectNode;

    /**
     * autoswitch默认的root目录
     * 实例化时，不设置root，则使用默认的root路径。
     */
    private String root = "/utils/autoswitch";

    private String projectFullPath;

    /**
     * 使用默认root构造自动切换实例
     *
     * @param connectString 连接zookeeper的ip端口号字符串，格式：hostname:port
     * @param project       当前需要做自动切换的工程名
     * @param projectNode   当前节点在原project中的nodename
     */
    public AutoSwitch(String connectString, String project, String projectNode) {
        this.project = project;
        this.projectFullPath = root + "/" + project;
        this.projectNode = projectNode;

        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(60 * 1000)
                .build();
    }

    /**
     * 创建自动切换实例
     *
     * @param connectString 连接zookeeper的ip端口号字符串，格式：hostname:port
     * @param root          自动切换在zookeeper中的root路径
     * @param project       当前需要做自动切换的工程名
     * @param projectNode   当前节点在原project中的nodename
     */
    public AutoSwitch(String connectString, String root, String project, String projectNode) {
        this.project = project;
        this.root = root;
        this.projectFullPath = root + "/" + project;
        this.projectNode = projectNode;

        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(60 * 1000)
                .build();
    }

    /**
     * 创建自动切换实例
     *
     * @param client      使用curator框架的zookeeper连接
     * @param root        自动切换在zookeeper中的root路径
     * @param project     当前需要做自动切换的工程名
     * @param projectNode 当前节点在原project中的nodename
     */
    public AutoSwitch(CuratorFramework client, String root, String project, String projectNode) {
        this.project = project;
        this.root = root;
        this.projectFullPath = root + "/" + project;
        this.projectNode = projectNode;
        this.client = client;
    }


    class PathChildCacheListenerAdapter implements PathChildrenCacheListener {

        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            logger.info("in project node " + projectNode);
            switch (event.getType()) {
                /**
                 * autoswitch标志节点 初始化成功
                 * 此时pathChildrenCacheEvent.getData()为空
                 */
                case INITIALIZED:
                    logger.info(project + "-" + projectNode + ": node initialized!");
                    if (ephemeralOwnerIsCurrentConnection()) {
                        countDownLatch.countDown();
                    }
                    break;
                /**
                 * autoswitch标志节点 成功增加节点，表示该节点能运行任务
                 * 此时触发任务。
                 */
                case CHILD_ADDED:
                    logger.info(project + "-" + projectNode + ": node added! " +
                            "node.path=" + new String(event.getData().getPath()) + ", " +
                            "node.date=" + new String(event.getData().getData()) + ", " +
                            "node.stat" + event.getData().getStat());
                    if (ephemeralOwnerIsCurrentConnection()) {
                        countDownLatch.countDown();
                    }
                    break;

                case CHILD_REMOVED:
                    logger.warn(project + "-" + projectNode + ": node removed! create node by " + projectNode);
                    createNode(CreateMode.EPHEMERAL);
                    break;
                case CHILD_UPDATED:
                    logger.info(project + "-" + projectNode + ": node updated! " +
                            "node.path=" + new String(event.getData().getPath()) + ", " +
                            "node.date=" + new String(event.getData().getData()) + ", " +
                            "node.stat" + event.getData().getStat());
                    break;
                /**
                 * 网络连接异常，应该主动断开连接
                 * 断开连接后，节点会自动消失
                 */
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED:
                    logger.info(project + "-" + projectNode + ": node connection error!, auto close zookeeper client");
                    client.close();
                    break;
                case CONNECTION_RECONNECTED:
                    logger.info(project + "-" + projectNode + ": node reconnected! " +
                            "node.path=" + new String(event.getData().getPath()) + ", " +
                            "node.date=" + new String(event.getData().getData()) + ", " +
                            "node.stat" + event.getData().getStat());
                    if (null != pathChildrenCache) {
                        logger.warn("rebuild path children cache");
                        pathChildrenCache.rebuild();
                    } else {
                        logger.warn("path children cache is null, need to renew path children cache and watch event!");
                        pathChildrenCache = new PathChildrenCache(client, root, true);
                        pathChildrenCache.getListenable().addListener(new PathChildCacheListenerAdapter());
                    }
                    break;
            }

        }
    }

    public void init() {
        this.client.start();
        try {
            Stat stat = this.client.checkExists().forPath(this.projectFullPath);
            if (null == stat) {
                /**
                 * 创建autoswitch工作节点，该节点是临时节点.
                 */
                createNode(CreateMode.EPHEMERAL);
            } else {
                logger.info(project + "-" + projectNode + ": node exists! add watcher");
            }
        } catch (Exception e) {
            logger.error(project + "-" + projectNode + ": create node error, node path:" + projectFullPath, e);
        } finally {
            watch();
        }

        logger.info(project + "-" + projectNode + ":auto switch start working...");

        try {
            logger.info(project + "-" + projectNode + ":waiting for switch to this progress...");
            countDownLatch.await();
            logger.info(project + "-" + projectNode + ": get right, start to run progress!");
        } catch (InterruptedException e) {
            logger.error(project + "-" + projectNode + ":countDownLatch intterupted error!", e);
        }
    }

    /**
     * 检查并创建节点
     *
     * TODO: 怎么解决自动切换过程中，如果两节点同时发现节点不存在，均发起创建节点请求时，可能会造成一个请求异常的情况。
     * 这个异常虽然对逻辑没有影响，但是不优雅
     * @param mode 节点的类型
     */
    private void createNode(CreateMode mode) {
        logger.info(project + "-" + projectNode + ": try create node ");
        try {
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(projectFullPath);
        } catch (Exception e) {
            logger.error("create " + projectFullPath + " node error ", e);
        }

    }

    private boolean ephemeralOwnerIsCurrentConnection() {
        boolean res = false;
        //获取临时节点和本连接的sessin标识。
        Stat stat = new Stat();
        try {
            client.getData().storingStatIn(stat).forPath(projectFullPath);
            res = ephemeralOwnerIsCurrentConnection(stat);
        } catch (Exception e) {
            logger.error("get " + projectFullPath + " data error ", e);
        }

        return res;
    }

    private boolean ephemeralOwnerIsCurrentConnection(Stat stat) {
        boolean res = false;
        if (null != stat) {
            try {
                long currentSessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                logger.info("node " + projectFullPath + " ephemeral Owner is " + stat.getEphemeralOwner() + ", currentSessionId is " + currentSessionId);

                //判断创建临时节点的连接是不是本连接
                if (stat.getEphemeralOwner() == currentSessionId) {
                    logger.info("ephemeralOwner is current connection! current session id " + currentSessionId);
                    res = true;
                } else {
                    logger.info("ephemeralOwner is not current connection! current session id " + currentSessionId + ", while ephmeralOwner is " + stat.getEphemeralOwner());
                }
            } catch (Exception e) {
                logger.error("get current connection sessionId error ", e);
            }
        }
        return res;
    }

    private void watch() {
        pathChildrenCache = new PathChildrenCache(this.client, root, true);
        boolean start = false;
        try {
            //创建本地缓存，并监听节点事件
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            start = true;
        } catch (Exception e) {
            logger.error(project + "-" + projectNode + ": pathChildrenCache start after initialized error", e);
        } finally {
            if (!start) {
                try {
                    pathChildrenCache.start();
                    pathChildrenCache.getListenable().addListener(new PathChildCacheListenerAdapter());
                } catch (Exception e) {
                    logger.error(project + "-" + projectNode + ": pathChildrenCache start error", e);
                }
            } else {
                logger.debug(project + "-" + projectNode + ": start listener.");
                pathChildrenCache.getListenable().addListener(new PathChildCacheListenerAdapter());
            }
        }
    }

}

