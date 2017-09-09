package win.hgfdodo.example.basic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by guangfuhe on 2017/9/2.
 */
class NodeCacheListenerAdapter implements NodeCacheListener {
    private Logger logger = LoggerFactory.getLogger(NodeCacheListenerAdapter.class);

    private CuratorFramework client;
    private String node;

    /**
     * @param client    already started client connection
     * @param node      node path of cached node
     */
    public NodeCacheListenerAdapter(CuratorFramework client, String node) {
        this.client = client;
        this.node = node;
    }

    public void nodeChanged() throws Exception {
        Stat stat = new Stat();
        byte[] data = this.client.getData().forPath(node);
        logger.info("node changed, data is "+ new String(data)+ ", stat is "+ stat.toString());
    }
}