package init;

import ads.sjtu.edu.cn.Percolator.model.Conf;
import ads.sjtu.edu.cn.Percolator.model.Transaction;
import ads.sjtu.edu.cn.Percolator.model.Write;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author 丁峰
 * @date 2018/3/30 16:50
 * @see InitData
 */
public class InitData {
    public static String[] accounts = {"chenbo", "xuhuatao", "huangsi", "qingpeijie"};
    public static Long[] datas = {100000l, 100000l, 100000l, 100000l, 100000l};

    public static void main(String[] args) throws IOException {
        createTable();
        insertData();
    }

    public static void createTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if(admin.tableExists("account_table")){
            admin.disableTable(Bytes.toBytes("account_table"));
            admin.deleteTable(Bytes.toBytes("account_table"));
        }

        HTableDescriptor htd = new HTableDescriptor("account_table");
        HColumnDescriptor hcd = new HColumnDescriptor("account");
        htd.addFamily(hcd);
        admin.createTable(htd);
        HTableDescriptor[] tables = admin.listTables();
        StringBuilder sb = new StringBuilder();
        for (HTableDescriptor hTableDescriptor : tables) {
            sb.append(hTableDescriptor.getNameAsString());
            sb.append("   ");
        }
        System.out.println("all table :");
        System.out.println(sb.toString());
    }

    public static void insertData() throws IOException {
        Transaction transaction = new Transaction();
        //创建
        for (int i = 0; i < accounts.length; ++i) {
            String account = accounts[i];
            Long data = datas[i];
            Write write = new Write(account, "account", data);
            transaction.addWrite(write);
        }
        transaction.commit();
        System.out.println("succeed to init data");
    }


}
