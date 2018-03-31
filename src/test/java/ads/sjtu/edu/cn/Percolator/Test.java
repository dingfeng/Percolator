package ads.sjtu.edu.cn.Percolator;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author 丁峰
 * @date 2018/3/30 9:13
 * @see Test
 */
public class Test {

    public static void main(String[] args) {
        try {
            addData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void addData() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        HTableDescriptor htd = new HTableDescriptor("test");
        HColumnDescriptor hcd = new HColumnDescriptor("data");
        htd.addFamily(hcd);
        admin.createTable(htd);
        byte[] tablename = htd.getName();
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length != 1 && new String(tablename, "utf8").equals("test")) {
            throw new IOException("Failed create of table");
        }
        HTable table = (HTable) connection.getTable(TableName.valueOf(Bytes.toBytes("test")));
        byte[] row1 = Bytes.toBytes("row1");
        Put p1 = new Put(row1);

        byte[] databytes = Bytes.toBytes("data");
        p1.addColumn(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
        table.put(p1);
        Get g = new Get(row1);
        Result result = table.get(g);
        System.out.println("Get: " + new String(result.getValue(Bytes.toBytes("data"),Bytes.toBytes("1")),"utf8"));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        try {
            for (Result scannerResult : resultScanner) {
                System.out.println("Scan: " + scannerResult);
            }
        } finally {
            resultScanner.close();
        }
//        admin.disableTable(tablename);
//        admin.deleteTable(tablename);
    }
}
