package win.hgfdodo.example.original;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by guangfuhe on 2017/9/2.
 */
public class OriginalWatcher {
    private ZooKeeper client;

    public OriginalWatcher(String connectString) throws IOException {
        this.client = new ZooKeeper(connectString, 6000, new Watcher() {
            public void process(WatchedEvent event) {
                switch (event.getType()){
                    case EventType.
                }
            }
        });
    }
}
