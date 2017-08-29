package win.hgfdodo.framework.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author guangfuhe 2017/8/28
 */
public class ResourcePool {
    private Logger logger = LoggerFactory.getLogger(ResourcePool.class);

    private String projectName;
    private String nodeName;

    private int resourceNum;
    /**
     * 默认的工作路径
     */
    private static String root = "/utils/resourcepoll";
    private String projectPath;

    private CuratorFramework client;

    public ResourcePool(String project, String nodeName, CuratorFramework client, int resourceNum) {
        this.projectName = project;
        this.nodeName = nodeName;
        this.projectPath = root + "/" + project;
        this.client = client;
        this.resourceNum = resourceNum;
    }

    public void init() {
        this.client.start();
        InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(client, projectPath, resourceNum);
    }
}
