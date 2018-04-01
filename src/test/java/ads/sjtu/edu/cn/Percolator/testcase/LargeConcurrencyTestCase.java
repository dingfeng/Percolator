package ads.sjtu.edu.cn.Percolator.testcase;

import ads.sjtu.edu.cn.Percolator.Conf;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import ads.sjtu.edu.cn.Percolator.service.AccountService;
import ads.sjtu.edu.cn.Percolator.serviceImpl.AccountServiceImpl;
import ads.sjtu.edu.cn.Percolator.timerImpl.WorkerImpl;
import ads.sjtu.edu.cn.Percolator.transaction.RowTransaction;
import ads.sjtu.edu.cn.Percolator.transaction.Transaction;
import ads.sjtu.edu.cn.Percolator.transaction.Write;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 丁峰
 * @date 2018/4/1 13:31
 * @see LargeConcurrencyTestCase
 */
public class LargeConcurrencyTestCase {
    public static final int WRITE_SIZE = 10;
    public static AtomicLong[] timeRecords = new AtomicLong[WRITE_SIZE + 2];  //时间记录
    public static AtomicInteger[] successCounts = new AtomicInteger[WRITE_SIZE + 2];  //成功用例的数量
    public static AtomicInteger[] failureCounts = new AtomicInteger[WRITE_SIZE + 2];  //失败用例的数量
    public static final int ACCOUNT_LENGTH = 200;  //200个数据规模
    public static String[] accounts = new String[ACCOUNT_LENGTH];
    public static AtomicLong[] accountDatas = new AtomicLong[ACCOUNT_LENGTH];
    public static final Long ACCOUNT_AMOUNT = 100000L;
    public static final int EXECUTE_COUNT_PER_THREAD = 200;
    public static final long TRANSFER_AMOUNT = 1;
    public static Map<String, Integer> accountMap = new ConcurrentHashMap<>();
    public static final int WORKTASK_SIZE = 3;
    public static WorkTask[] workTasks = new WorkTask[WORKTASK_SIZE];

    public static void main(String[] args) throws IOException, InterruptedException {
        initTest();
        setTestData();
        test();
        boolean validateResult = validate();
        if (validateResult) {
            System.out.println("validate success");
        } else {
            System.out.println("validate failure");
        }
        removeTestData();

    }

    public static void test() throws InterruptedException {
        for (int i = 0; i < WORKTASK_SIZE; ++i) {
            workTasks[i] = new WorkTask();
            workTasks[i].setWorkNo(i);
            workTasks[i].start();
        }
        for (int i = 0; i < WORKTASK_SIZE; ++i) {
            workTasks[i].join();
        }
        printResult();
    }

    public static boolean validate() throws IOException {
        Transaction transaction = new Transaction();
        //get validate account one by one
        for (int i = 0; i < ACCOUNT_LENGTH; ++i) {
            String account = accounts[i];
            long realAmount = accountDatas[i].longValue();
            long dbAmount = transaction.get(account, Transaction.ACCOUNT_FAMILY);
            if (realAmount != realAmount) {
                System.out.println("invalid data for account " + account + " realAmount=" + realAmount + " dbAmount=" + dbAmount);
                return false;
            }
        }
        return true;
    }

    public static void printResult() {
        StringBuilder sb = new StringBuilder();
        //print time records
        sb.append("timeRecord\n");
        sb.append(Arrays.toString(timeRecords));
        sb.append("\n");
        sb.append("successCounts\n");
        sb.append(Arrays.toString(successCounts));
        sb.append("\n");
        sb.append("failureCounts\n");
        sb.append(Arrays.toString(failureCounts));
        String result = sb.toString();
        System.out.println(result);
    }

    static class WorkTask extends Thread {
        AccountService accountService = new AccountServiceImpl();
        private int workNo;

        public void setWorkNo(int workNo) {
            this.workNo = workNo;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < EXECUTE_COUNT_PER_THREAD; ++i) {
                    System.out.println("workNo = " + workNo + " i= " + i);
                    oneExecute();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void oneExecute() throws IOException {
            List<String> accountList = getRandomAccounts();
            String outAccount = accountList.get(0);
            List<TransferAccountItemParam> transferAccountItemParamList = new ArrayList<>();
            for (int i = 1; i < accountList.size(); ++i) {
                TransferAccountItemParam transferAccountItemParam = new TransferAccountItemParam();
                String inAccount = accountList.get(i);
                transferAccountItemParam.setAccount(inAccount);
                transferAccountItemParam.setAmount(TRANSFER_AMOUNT);
                transferAccountItemParamList.add(transferAccountItemParam);
            }
            long startTime = System.currentTimeMillis();
            boolean result = accountService.transferAccount(outAccount, transferAccountItemParamList);
            long executeTime = System.currentTimeMillis() - startTime;
            int writeSize = accountList.size();
            timeRecords[writeSize].addAndGet(executeTime);
            if (result) {
                //update accountData
                int outAccountIndex = accountMap.get(outAccount);
                accountDatas[outAccountIndex].addAndGet(-TRANSFER_AMOUNT * (accountList.size() - 1));
                for (int i = 1; i < accountList.size(); ++i) {
                    String inAccount = accountList.get(i);
                    int inAccountIndex = accountMap.get(inAccount);
                    accountDatas[inAccountIndex].addAndGet(TRANSFER_AMOUNT);
                }
                successCounts[writeSize].addAndGet(1);
            } else {
                failureCounts[writeSize].addAndGet(1);
            }
        }
    }

    public static List<String> getRandomAccounts() {
        Random rand = new Random();
        int writeNum = rand.nextInt(WRITE_SIZE) + 2;
        Set<String> selectedAccountSet = new HashSet<>(writeNum);
        List<String> selectedAccountList = new ArrayList<>(writeNum);
        while (selectedAccountSet.size() < writeNum) {
            int accountIndex = rand.nextInt(ACCOUNT_LENGTH);
            String account = accounts[accountIndex];
            if (selectedAccountSet.add(account)) {
                selectedAccountList.add(account);
            }
        }
        return selectedAccountList;
    }

    public static String getUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static void initTest() throws IOException {
        //generate random account
        for (int i = 0; i < ACCOUNT_LENGTH; ++i) {
            accounts[i] = getUUID();
            accountMap.put(accounts[i], i);
        }
        //init census array
        for (int i = 0; i < timeRecords.length; ++i) {
            timeRecords[i] = new AtomicLong(0);
            successCounts[i] = new AtomicInteger(0);
            failureCounts[i] = new AtomicInteger(0);
        }
    }


    private static void insertData() throws IOException {
        Transaction transaction = new Transaction();
        //创建
        for (int i = 0; i < accounts.length; ++i) {
            String account = accounts[i];
            accountDatas[i] = new AtomicLong(ACCOUNT_AMOUNT);
            Write write = new Write(account, "account", ACCOUNT_AMOUNT);
            transaction.addWrite(write);
        }
        transaction.commit();
        System.out.println("succeed to init name data");
    }


    public static void setTestData() throws IOException {
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
        insertData();
    }

    public static void removeTestData() throws IOException {
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
    }

}
