package ads.sjtu.edu.cn.Percolator;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableMap;

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
        HColumnDescriptor accountHcd = new HColumnDescriptor("name");
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
    }

    static String allTableData(String tablename) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HTable htable = (HTable) connection.getTable(TableName.valueOf(tablename.getBytes()));
        Scan scan = new Scan();
        scan.setMaxVersions();
        ResultScanner resultScanner = htable.getScanner(scan);
        StringBuilder sb = new StringBuilder();
        sb.append("------------------------------------\n");
        for (Result scannerResult : resultScanner) {
            byte[] row = scannerResult.getRow();
            sb.append("\n");
            sb.append(Bytes.toString(row));
            sb.append("\n");
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = scannerResult.getMap();
//            logger.info(allVersions.toString());
            for (byte[] keySet : allVersions.keySet()) {
                String keyStr = Bytes.toString(keySet);
                sb.append(keyStr);
                sb.append(":");
                NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = allVersions.get(keySet);
                for (byte[] colMapKeySet : colMap.keySet()) {
                    sb.append(Bytes.toString(colMapKeySet));
                    sb.append(colMap.get(colMapKeySet));
                    sb.append("\n");
                }
            }
        }
        sb.append("-------------------------------------\n");
        return sb.toString();
    }

    static void testGet() {

    }

    public static void main(String[] args) throws IOException {
        logger.info(allTableData("test"));
    }
}
