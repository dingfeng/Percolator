package ads.sjtu.edu.cn.Percolator;

import ads.sjtu.edu.cn.Percolator.timerImpl.WorkerImpl;
import ads.sjtu.edu.cn.Percolator.transaction.Transaction;
import ads.sjtu.edu.cn.Percolator.transaction.Write;
import com.google.common.base.Throwables;
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
//        new DataAddTask("task1").start();
//        new DataAddTask("task2").start();
        initData();
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


    public static void addRecordData() throws IOException {
        Transaction transaction = new Transaction(Conf.RECORD_TABLE);
        String col = "record";
        Long[] updatedDatas = new Long[accounts.length];
        for (int i = 0; i < accounts.length; ++i) {
            String accountKey = accounts[i];
            Long accoutData = transaction.get(accountKey, col);
            accoutData = accoutData == null ? -1 : accoutData;
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
        insertRecordData();
    }

    private static void createTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists("account_table")) {
            admin.disableTable(Bytes.toBytes("account_table"));
            admin.deleteTable(Bytes.toBytes("account_table"));
        }
        if (admin.tableExists("record_table")) {
            admin.disableTable(Bytes.toBytes("record_table"));
            admin.deleteTable(Bytes.toBytes("record_table"));
        }
        HTableDescriptor accountHtd = new HTableDescriptor("account_table");
        HColumnDescriptor accountHcd = new HColumnDescriptor("account");
        HColumnDescriptor accountNotification = new HColumnDescriptor("notification");
        accountHtd.addFamily(accountHcd);
        accountHtd.addFamily(accountNotification);
        admin.createTable(accountHtd);
        HTableDescriptor rankHtd = new HTableDescriptor("record_table");
        HColumnDescriptor rankHcd = new HColumnDescriptor("record");
        HColumnDescriptor rankNotification = new HColumnDescriptor("notification");
        rankHtd.addFamily(rankHcd);
        rankHtd.addFamily(rankNotification);
        admin.createTable(rankHtd);
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
        System.out.println("succeed to init account data");
    }

    private static void insertRecordData() throws IOException {
        Transaction transaction = new Transaction(Conf.RECORD_TABLE);
        //创建
        for (int i = 0; i < accounts.length; ++i) {
            String account = accounts[i];
            Write write = new Write(account, "record", -1l);
            transaction.addWrite(write);
        }
        Write upWrite = new Write(WorkerImpl.UP_COUNT_KEY, "record", 0l);
        transaction.addWrite(upWrite);
        Write downWrite = new Write(WorkerImpl.DOWN_COUNT_KEY, "record", 0l);
        transaction.addWrite(downWrite);
        transaction.commit();
        System.out.println("succeed to init  record data");
    }


}
