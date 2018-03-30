package ads.sjtu.edu.cn.Percolator.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author 丁峰
 * @date 2018/3/28 20:59
 * @see Conf
 */
public class Conf {
    public static String MASTER_IP = "192.168.0.201";
    public static String SUPPORT_SERVER_NAME = "SupportServer";
    public static String ZOOKEEPER_ADDR = "master,slave1,slave2";
    public static Configuration HBASE_CONFIG ;
    public static int CLIENTPORT = 2181;

    static {
        HBASE_CONFIG = HBaseConfiguration.create();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", ZOOKEEPER_ADDR);
        HBASE_CONFIG.set("hbase.rootdir", "hdfs://master:9000/hbase");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", String.valueOf(CLIENTPORT));

    }
}
