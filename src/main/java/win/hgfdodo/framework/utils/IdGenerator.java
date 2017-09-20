package win.hgfdodo.framework.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class IdGenerator {
    private final static Logger logger = LoggerFactory.getLogger(IdGenerator.class);

    private final static int RADIX = 62;
    /**
     * 生成ID的默认长度
     */
    private final static int DEFAULT_ID_LENGTH = 6;
    private final static char[] symbols = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};

    /**
     * zookeeper的连接
     */
    private CuratorFramework client;
    /**
     * 使用zookeeper的路径
     */
    private String root;
    /**
     * 是否定长
     */
    private boolean fixedLength;
    /**
     * ID的长度，在启动ID固定长度时起作用
     */
    private int length = DEFAULT_ID_LENGTH;
    /**
     * 自增区间
     */
    private int interval;
    /**
     * 从zookeeper中获取检查点，生成的ID范围是
     */
    private long start = 0;
    /**
     * 上次生成id时的值
     */
    private long last = 0;

    /**
     * 多个客户端间的锁
     */
    private InterProcessMutex lock;

    public IdGenerator(String connectString, String root, int interval) {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(6000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.root = root;
        this.interval = interval;
    }

    public IdGenerator(CuratorFramework client, String root, int interval) {
        this(client, root, true, interval);
    }

    public IdGenerator(CuratorFramework client, String root, boolean fixedLength, int interval) {
        this.client = client;
        this.root = root;
        this.fixedLength = fixedLength;
        this.interval = interval;
    }

    public IdGenerator(CuratorFramework client, String root, boolean fixedLength, int length, int interval) {
        this.client = client;
        this.root = root;
        this.fixedLength = fixedLength;
        this.length = length;
        this.interval = interval;
    }

    public IdGenerator(String connectString, String root, boolean fixedLength, int length, int interval) {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(6000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.root = root;
        this.fixedLength = fixedLength;
        this.length = length;
        this.interval = interval;
    }

    /**
     * 初始化zookeeper链接，获取start和区间
     * <p>
     * 事务操作：获取当前节点的值v，更新当前值为v+interval，则本id生成器的id初始值从v+1开始到v+interval。
     */
    public void init() {
        client.start();
        lock = new InterProcessMutex(client, root);
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(root, "0".getBytes());
        } catch (Exception e) {
            logger.error("create node root={} error!", root);
        }
        start = requestAndGenerateFromZookeeper();
        last = start;
    }


    /**
     * start生成ID
     * <p>
     * 生成的ID必须保证在范围内，查过范围需请求协商。
     *
     * @return
     */
    public String generateId() {
        long toGenerateValue = ++last;
        if (toGenerateValue > start + interval) {
            toGenerateValue = requestAndGenerateFromZookeeper()+1;
            last = toGenerateValue;
            logger.info("generate from zk once: "+ transToSymbol(toGenerateValue));
        }
        return transToSymbol(toGenerateValue);
    }

    public long requestAndGenerateFromZookeeper() {
        try {
            lock.acquire(2, TimeUnit.SECONDS);
            byte[] data = client.getData().forPath(root);
            long point = Long.parseLong(new String(data));
            client.setData().forPath(root, String.valueOf(point + interval).getBytes());
            start = point;
        } catch (Exception e) {
            logger.error("acquired lock to generate new id intervals error! ", e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                logger.error("release lock error!", e);
            }
        }
        return start;
    }

    public String transToSymbol(long toGenerateValue) {
        StringBuilder sb = new StringBuilder();
        while (toGenerateValue > 0) {
            sb.insert(0, symbols[(int) (toGenerateValue % symbols.length)]);
            toGenerateValue /= symbols.length;
        }
        if (fixedLength) {
            return toFixedLength(sb);
        }

        return sb.toString();
    }


    /**
     * 将value转为固定长度
     *
     * @param value
     * @return
     */
    private String toFixedLength(StringBuilder value) {
        for (int i = value.length(); i < length; i++) {
            value.insert(0, "0");
        }
        return value.toString();
    }

    public int getInterval() {
        return interval;
    }

    public long getStart() {
        return start;
    }

    public long getLast() {
        return last;
    }
}
