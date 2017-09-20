package win.hgfdodo.framework.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class IdGeneratorTest implements Callable<List<String>> {
    private final static Logger log = LoggerFactory.getLogger(IdGeneratorTest.class);
    private CuratorFramework client;
    private IdGenerator idGenerator;
    private List<String> list = new ArrayList<String>();
    private Set<String> set = new HashSet<String>();

    public IdGeneratorTest() {
        client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(6000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        idGenerator = new IdGenerator(client, "/test", 100);
        idGenerator.init();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        int thread = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(thread);
        List<Future<List<String>>> f = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < thread; i++) {
            f.add(executorService.submit(new IdGeneratorTest()));
        }

        HashSet<String> set = new HashSet<String>();
        for (int i = 0; i < thread; i++) {
            List<String> list = f.get(i).get();
            for (String str : list) {
                System.out.println(str);
                if (!set.add(str)) {
                    log.error("ERROR: " + str);
                }
            }
        }
        executorService.shutdown();

    }


    public List<String> call() throws Exception {
        try {
            Random random = new Random();
            for (int i = 0; i < 200; i++) {
                Thread.sleep(random.nextInt(200));
                String id = idGenerator.generateId();

                list.add(Thread.currentThread().getId() + ",\t" + id + "\t" + idGenerator.transToSymbol(idGenerator.getStart()) + "\t" + idGenerator.getInterval() + "\t" + idGenerator.transToSymbol(idGenerator.getLast()));
                if (!set.add(id)) {
                    log.error("Duplicated:" + id);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

}