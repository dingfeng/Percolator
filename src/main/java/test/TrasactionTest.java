package test;

import ads.sjtu.edu.cn.Percolator.model.Conf;
import ads.sjtu.edu.cn.Percolator.model.SupportServiceClient;
import ads.sjtu.edu.cn.Percolator.model.Transaction;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author 丁峰
 * @date 2018/3/30 10:39
 * @see TrasactionTest
 */
public class TrasactionTest {
    static Logger logger = LoggerFactory.getLogger(TrasactionTest.class);

    static void addData() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        HTableDescriptor htd = new HTableDescriptor("account_table");
        HColumnDescriptor accountHcd = new HColumnDescriptor("account");
        HColumnDescriptor notificationHcd = new HColumnDescriptor("notification");
        htd.addFamily(accountHcd);
        htd.addFamily(notificationHcd);
        admin.createTable(htd);
        byte[] tablename = htd.getName();
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor hTableDescriptor : tables) {
            String hTableName = hTableDescriptor.getNameAsString();
            logger.info("htable= {}", hTableName);
        }
//        SupportServiceClient supportServiceClient = SupportServiceClient.getInstance();
        //添加数据
        Transaction transaction = new Transaction("account_table");
    }

    static String allTableData(String tablename) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HTable htable = (HTable) connection.getTable(TableName.valueOf(tablename.getBytes()));
        Scan scan = new Scan();
        scan.setMaxVersions();
        ResultScanner resultScanner = htable.getScanner(scan);
        for (Result scannerResult : resultScanner) {

        }
        return null;
    }

    static void testGet() {

    }
}
