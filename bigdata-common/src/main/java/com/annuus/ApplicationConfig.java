package com.annuus;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 应用全局配置中心
 * User: shuiqing
 * Date: 16/4/24
 * Time: 下午5:46
 * Email: annuus.sq@gmail.com
 * GitHub: https://github.com/shuiqing301
 * Blog: http://shuiqing301.github.io/
 * _
 * |_)._ _
 * | o| (_
 */
public class ApplicationConfig implements Serializable {
    private static final long serialVersionUID = 6211344970724898565L;

    public final static int SESSION_TIMEOUT_TIME = 30 * 60;  //秒级别，会话超时时间，默认30分钟

    /**
     * hdfs地址
     */
    private String hdfsClusterName;		//hdfs的集群名
    private String hdfsAddr;			//hdfs namenode, secondary namenode地址，ip:port,ip:port

    /**
     * hbase地址
     */
    private String zookeeper;			//zookeeper地址
    private int hbasePoolSize;			//hbase线程池大小
    private int hbaseHConnectionNum;	//hbase hconnection数量

    /**
     * api端口
     */
    private int apiThriftPort;			//api thrift端口

    /**
     * 实时统计topology配置
     */
    private int statWorks;				//统计进程数
    private int sessionSearchNum;		//过期会话查询executor数
    private int idConvertNum;			//id转化executor数
    private int kpiStatsNum;			//apache统计计算executor数
    private int statsStoreNum;			//kpi统计存储executor数
    private int jobStatsNum;			//job统计存储executor数
    private int hdfsStoreNum;			//hdfs存储的executor数
    private int cacheNum;				//cacheBolt(缓存)的executor数

    private String stormWorkChildOpts;  //设置work的内存为8GB


    /**
     * 批量配置
     */
    private int batchCount;				//批量处理数量
    private int batchTimeGap;			//批量处理间隔时间
    private int batchTimePerid;			//定时检查是否需要处理
    private boolean batchIsExecuted;	//是否进行批量处理

    private long cacheKeysSize;			//最大缓存数量
    private long executorPeriod;		//KpiStore top flush定时器时间间隔,单位毫秒
    private long cacheBoltExecutorPeriod;//CacheStore top 计算及发送详情数据定时器时间间隔,单位毫秒
    /**
     * hash操作
     */
    private int searchValueHashNum;				//Hash数目  searchValue
    /**
     * mapreduce配置
     */
    private long  mrInputSplitSize;     //mapreduce中map输入分片的最大大小(单位：byte)
    private int   mrMaxMapNum;          //mapreduce中map的最大个数
    private float mrMapReduceNumRate;   //mapreduce中map个数与reduce个数的比值

    /***
     * 初始化全局配置
     * @param filePath   配置文件路径
     * @throws IOException
     */
    private ApplicationConfig(String filePath) throws IOException {
        RichProperties props = new RichProperties();
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
            props.load(is);
        } finally {
            try {
                if (is != null) is.close();
            } catch (IOException e) { }
        }

        this.hdfsClusterName = props.getString("hdfs.cluster.name");
        this.hdfsAddr = props.getString("hdfs.address");

        this.zookeeper = props.getString("hbase.zookeeper.quorum");
        this.hbasePoolSize = props.getInt("hbase.pool.size", 15);	//hbase线程池大小默认为15
        this.hbaseHConnectionNum = props.getInt("hbase.hconnection.num", 1);

        this.apiThriftPort = props.getInt("web.stats.service.thrift.port", 55555);

        this.statWorks = props.getInt("storm.stat.works", 3);
        this.kpiStatsNum = props.getInt("storm.stat.kpiStatsBolt.executes", 3);
        this.statsStoreNum = props.getInt("storm.stat.kpiStoreBolt.executes", 3);
        this.sessionSearchNum  = props.getInt("storm.stat.sessionSearchBolt.executes", 3);
        this.idConvertNum = props.getInt("storm.stat.idConvertBolt.executes", 3);
        this.jobStatsNum = props.getInt("storm.stat.jobBolt.executes", 3);
        this.cacheNum = props.getInt("storm.stat.cacheBolt.executes", 3);
        this.hdfsStoreNum = props.getInt("storm.stat.hdfsBolt.executes", 3);
        this.stormWorkChildOpts = props.getString("storm.worker.childopts");

        this.batchCount = props.getInt("storm.stats.batch.count", 100);	//默认1000条tuple
        this.batchTimeGap = props.getInt("storm.stats.batch.time.gap", 500); //默认500毫秒
        this.batchTimePerid = props.getInt("storm.stats.batch.time.perid", 2000); //默认2秒
        this.batchIsExecuted = props.getBoolean("storm.stats.batch.isExecute", true); //默认500毫秒
        this.searchValueHashNum = props.getInt("storm.searchValueHash.size", 10); //默认20

        this.mrInputSplitSize = Long.parseLong(props.getString("mapreduce.input.split.size", "2000000000"));     //默认2G
        this.mrMaxMapNum = props.getInt("mapreduce.map.tasks.maxnum", 8);	               						 //默认8个map
        this.mrMapReduceNumRate = Float.parseFloat(props.getString("mapreduce.mapandreduce.tasks.rate", "0.3")); //默认0.3
    }

    /**
     * 功能：初始化配置
     * @throws IOException
     */
    public static ApplicationConfig loadConfig() throws IOException{
        //读取执行程序所在目录，获取配置文件路径
        File directory	= new File(".");
        String thisDirPath = directory.getCanonicalPath();
        if(thisDirPath.startsWith("/")){
            int index = thisDirPath.indexOf("bigdata-common");
            return new ApplicationConfig(thisDirPath.substring(0, index + "bigdata-common".length()) + "/conf/config.properties");
        } else {
            return new ApplicationConfig("/conf/config.properties");
        }
    }

    /***
     * 自定义类对象，用于方便根据类型获取配置属性值
      */
    public static class RichProperties extends Properties {
        private static final long serialVersionUID = 1L;

        public Integer getInt(String name) {
            return getInt(name, null);
        }

        public Integer getInt(String name, Integer defaultValue) {
            String value = super.getProperty(name);
            if (value == null) {
                return defaultValue;
            } else {
                return Integer.parseInt(value.trim());
            }
        }

        public Double getDouble(String name) {
            return getDouble(name, null);
        }

        public Double getDouble(String name, Double defaultValue) {
            String value = super.getProperty(name);
            if (value == null) {
                return defaultValue;
            } else {
                return Double.parseDouble(value.trim());
            }
        }

        public String getString(String name) {
            return getString(name, null);
        }

        public String getString(String name, String defaultValue) {
            String value = super.getProperty(name);
            if (value == null) {
                return defaultValue;
            } else {
                return value.trim();
            }
        }

        /**
         * 功能：获取集合，各个属性用“，”分割
         *
         * @param name
         * @return
         */
        public List<String> getList(String name) {
            List<String> list = new ArrayList<String>();
            String value = super.getProperty(name);
            if (value != null) {
                for (String str : value.split(",")) {
                    list.add(str.trim());
                }
            }
            return list;
        }

        public boolean getBoolean(String name, boolean defaultValue) {
            String value = super.getProperty(name);
            if (value != null) {
                return Boolean.parseBoolean(value);
            }
            return defaultValue;
        }
    }

    public static int getSessionTimeoutTime() {
        return SESSION_TIMEOUT_TIME;
    }

    public String getHdfsClusterName() {
        return hdfsClusterName;
    }

    public void setHdfsClusterName(String hdfsClusterName) {
        this.hdfsClusterName = hdfsClusterName;
    }

    public String getHdfsAddr() {
        return hdfsAddr;
    }

    public void setHdfsAddr(String hdfsAddr) {
        this.hdfsAddr = hdfsAddr;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public int getHbasePoolSize() {
        return hbasePoolSize;
    }

    public void setHbasePoolSize(int hbasePoolSize) {
        this.hbasePoolSize = hbasePoolSize;
    }

    public int getHbaseHConnectionNum() {
        return hbaseHConnectionNum;
    }

    public void setHbaseHConnectionNum(int hbaseHConnectionNum) {
        this.hbaseHConnectionNum = hbaseHConnectionNum;
    }

    public int getApiThriftPort() {
        return apiThriftPort;
    }

    public void setApiThriftPort(int apiThriftPort) {
        this.apiThriftPort = apiThriftPort;
    }

    public int getStatWorks() {
        return statWorks;
    }

    public void setStatWorks(int statWorks) {
        this.statWorks = statWorks;
    }

    public int getSessionSearchNum() {
        return sessionSearchNum;
    }

    public void setSessionSearchNum(int sessionSearchNum) {
        this.sessionSearchNum = sessionSearchNum;
    }

    public int getIdConvertNum() {
        return idConvertNum;
    }

    public void setIdConvertNum(int idConvertNum) {
        this.idConvertNum = idConvertNum;
    }

    public int getKpiStatsNum() {
        return kpiStatsNum;
    }

    public void setKpiStatsNum(int kpiStatsNum) {
        this.kpiStatsNum = kpiStatsNum;
    }

    public int getStatsStoreNum() {
        return statsStoreNum;
    }

    public void setStatsStoreNum(int statsStoreNum) {
        this.statsStoreNum = statsStoreNum;
    }

    public int getJobStatsNum() {
        return jobStatsNum;
    }

    public void setJobStatsNum(int jobStatsNum) {
        this.jobStatsNum = jobStatsNum;
    }

    public int getHdfsStoreNum() {
        return hdfsStoreNum;
    }

    public void setHdfsStoreNum(int hdfsStoreNum) {
        this.hdfsStoreNum = hdfsStoreNum;
    }

    public int getCacheNum() {
        return cacheNum;
    }

    public void setCacheNum(int cacheNum) {
        this.cacheNum = cacheNum;
    }

    public String getStormWorkChildOpts() {
        return stormWorkChildOpts;
    }

    public void setStormWorkChildOpts(String stormWorkChildOpts) {
        this.stormWorkChildOpts = stormWorkChildOpts;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public int getBatchTimeGap() {
        return batchTimeGap;
    }

    public void setBatchTimeGap(int batchTimeGap) {
        this.batchTimeGap = batchTimeGap;
    }

    public int getBatchTimePerid() {
        return batchTimePerid;
    }

    public void setBatchTimePerid(int batchTimePerid) {
        this.batchTimePerid = batchTimePerid;
    }

    public boolean isBatchIsExecuted() {
        return batchIsExecuted;
    }

    public void setBatchIsExecuted(boolean batchIsExecuted) {
        this.batchIsExecuted = batchIsExecuted;
    }

    public long getCacheKeysSize() {
        return cacheKeysSize;
    }

    public void setCacheKeysSize(long cacheKeysSize) {
        this.cacheKeysSize = cacheKeysSize;
    }

    public long getExecutorPeriod() {
        return executorPeriod;
    }

    public void setExecutorPeriod(long executorPeriod) {
        this.executorPeriod = executorPeriod;
    }

    public long getCacheBoltExecutorPeriod() {
        return cacheBoltExecutorPeriod;
    }

    public void setCacheBoltExecutorPeriod(long cacheBoltExecutorPeriod) {
        this.cacheBoltExecutorPeriod = cacheBoltExecutorPeriod;
    }

    public int getSearchValueHashNum() {
        return searchValueHashNum;
    }

    public void setSearchValueHashNum(int searchValueHashNum) {
        this.searchValueHashNum = searchValueHashNum;
    }

    public long getMrInputSplitSize() {
        return mrInputSplitSize;
    }

    public void setMrInputSplitSize(long mrInputSplitSize) {
        this.mrInputSplitSize = mrInputSplitSize;
    }

    public int getMrMaxMapNum() {
        return mrMaxMapNum;
    }

    public void setMrMaxMapNum(int mrMaxMapNum) {
        this.mrMaxMapNum = mrMaxMapNum;
    }

    public float getMrMapReduceNumRate() {
        return mrMapReduceNumRate;
    }

    public void setMrMapReduceNumRate(float mrMapReduceNumRate) {
        this.mrMapReduceNumRate = mrMapReduceNumRate;
    }
}
