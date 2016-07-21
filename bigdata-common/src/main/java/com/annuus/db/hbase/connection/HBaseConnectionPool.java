package com.annuus.db.hbase.connection;

import com.annuus.ApplicationConfig;
import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HBase connection pool.
 * User: shuiqing
 * Date: 16/4/24
 * Time: 下午5:32
 * Email: annuus.sq@gmail.com
 * GitHub: https://github.com/shuiqing301
 * Blog: http://shuiqing301.github.io/
 * _
 * |_)._ _
 * | o| (_
 */
public class HBaseConnectionPool {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseConnectionPool.class);

    private static List<HConnection> connections = new ArrayList<HConnection>();	//容器，空闲连接
    private static List<ExecutorService> executorList = new ArrayList<ExecutorService>();
    private static AtomicInteger num = new AtomicInteger(0);
    private static int hbaseHConnectionNum;
    private static String zookeeper;
    private static int poolSize;

    /**
     * 功能：初始化hbase连接池
     * @param applicationConfig
     */
    public static void initHConnectionPool(ApplicationConfig applicationConfig) {
        initHConnectionPool(applicationConfig.getHbaseHConnectionNum(), applicationConfig.getZookeeper(), applicationConfig.getHbasePoolSize());
    }

    /**
     * 功能：初始化连接池
     * @param hbaseHConnectionNum	需要创建的HConnection个数
     * @param zookeeper			    zookeeper地址
     * @param poolSize				每个HConnection并发数
     */
    public static void initHConnectionPool(int hbaseHConnectionNum, String zookeeper, int poolSize) {
        if(connections == null || connections.size()  == 0){
            synchronized (HBaseConnectionPool.class) {
                if(connections == null || connections.size()  == 0){
                    for(int num = 0; num < hbaseHConnectionNum; num++){
                        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
                        executorList.add(pool);
                        connections.add(HbaseUtils.createHConnection(zookeeper, pool));
                    }
                    HBaseConnectionPool.hbaseHConnectionNum = hbaseHConnectionNum;
                    HBaseConnectionPool.zookeeper = zookeeper;
                    HBaseConnectionPool.poolSize = poolSize;
                }
            }
        }
    }

    /**
     * 从连接池里得到连接
     * @return
     */
    public static HConnection getConnection(){
        if(connections.size() == 0)
            initHConnectionPool(hbaseHConnectionNum, zookeeper, poolSize);
        int index = num.incrementAndGet() % hbaseHConnectionNum;
        return connections.get(index);
    }

    /**
     * 功能：关闭连接池
     */
    public static synchronized void close(){
        for(int i = 0; i < connections.size(); i++){
            try {
                connections.get(i).close();
            } catch (IOException e) {
                LOG.error("error to close hbase connection");
            }
        }
        for(int i = 0; i < executorList.size(); i++){
            executorList.get(i).shutdown();
        }
        connections.clear();
        executorList.clear();
    }
}
