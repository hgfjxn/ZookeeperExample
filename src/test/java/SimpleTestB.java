import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import win.hgfdodo.framework.utils.AutoSwitch;

/**
 * Created by guangfuhe on 2017/8/27.
 */
public class SimpleTestB {
    public static void main(String[] args) throws InterruptedException {
        String root = "/win/hgfdodo";
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(6000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        AutoSwitch autoswitch = new AutoSwitch(client, root, "test", "node2");
        autoswitch.init();
        System.out.println("working in B");
        while(true){
            Stat stat = new Stat();
            Thread.sleep(500);
            try {
                System.out.println(new String(client.getData().storingStatIn(stat).forPath(root+"/test")));
                System.out.println("stat:" + stat.toString());
                System.out.println("version:" + stat.getVersion());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
