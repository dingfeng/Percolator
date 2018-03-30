package init;

import ads.sjtu.edu.cn.Percolator.model.Conf;
import ads.sjtu.edu.cn.Percolator.model.Transaction;
import ads.sjtu.edu.cn.Percolator.model.Write;
import com.google.common.base.Throwables;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author 丁峰
 * @date 2018/3/30 16:50
 * @see InitData
 */
public class InitData {
    static Logger logger = LoggerFactory.getLogger(InitData.class);
    public static String[] accounts = {"chenbo", "xuhuatao", "huangsi", "qingpeijie"};
    public static Long[] datas = {100000l, 100000l, 100000l, 100000l, 100000l};

    public static void main(String[] args) throws IOException {
//        addAccountData();
        new DataAddTask("task1").start();
//        new DataAddTask("task2").start();
    }

    @Data
    static class DataAddTask extends Thread {
        private static final int ADD_COUNT = 100;
        private String taskName;

        public DataAddTask(String name) {
            super(name);
            this.taskName = name;
        }

        @Override
        public void run() {
            logger.info("task name={} started", this.taskName);
            for (int i = 0; i < ADD_COUNT; ++i) {
                logger.info("task name = {} count = {}", this.taskName, i);
                try {
                    addAccountData();
                } catch (Exception e) {
                    logger.error("name={} error={}", this.taskName, Throwables.getStackTraceAsString(e));
                }
            }
        }

    }


    public static void addAccountData() throws IOException {
        Transaction transaction = new Transaction();
        String col = "account";
        Long[] updatedDatas = new Long[accounts.length];
        for (int i = 0; i < accounts.length; ++i) {
            String accountKey = accounts[i];
            Long accoutData = transaction.get(accountKey, col);
            accoutData = accoutData == null ? 0 : accoutData;
            accoutData += 1;
            updatedDatas[i] = accoutData;
        }
        logger.info("updateDatas = {}", Arrays.toString(updatedDatas));
        //更新账户
        for (int i = 0; i < accounts.length; ++i) {
            String accountKey = accounts[i];
            Long updatedData = updatedDatas[i];
            Write write = new Write(accountKey, col, updatedData);
            transaction.addWrite(write);
        }
        transaction.commit();
    }

    public static void initData() throws IOException {
        createTable();
        insertData();
    }

    private static void createTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists("account_table")) {
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

    private static void insertData() throws IOException {
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
