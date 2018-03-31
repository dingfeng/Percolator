package ads.sjtu.edu.cn.Percolator.timerImpl;


import ads.sjtu.edu.cn.Percolator.component.ThreadPool;
import ads.sjtu.edu.cn.Percolator.timer.Worker;
import ads.sjtu.edu.cn.Percolator.transaction.*;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * @author 丁峰
 * @date 2018/3/31 13:27
 * @see WorkImpl
 */
@Component
public class WorkImpl implements Worker {
    static Logger logger = LoggerFactory.getLogger(WorkImpl.class);
    private static final int WORK_SIZE = 1000;
    private static final float RANDOM_FILTER_CHANCE = 1.0f / WORK_SIZE;
    private static final int RANDOM_PAGE_SIZE = 100;
    private static final String UP_COUNT_KEY = "up_count_key";
    private static final String DOWN_COUNT_KEY = "down_count_key";
    private static final String RECORD_TABLE_FAMILY = "record";
    @Autowired
    private ThreadPool threadPool;

    @Scheduled(cron = "* * * * * *")
    public void scanNotificationColumn() {
        logger.info("timer task scanNotificationColumn has been started");
        try {
            threadPool.startTask(new WorkTask());
        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        logger.info("timer task scanNotificationColumn finished");
    }

    static class WorkTask implements Runnable {

        @Override
        public void run() {
            List<Filter> filters = new ArrayList<>();
            byte[] startRow = getRandomRow();
            Filter singleColumnValueFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), Bytes.toBytes(Transaction.NOTIFICATION_FAMILY_FLAG), CompareFilter.CompareOp.EQUAL, new byte[]{1});
            filters.add(singleColumnValueFilter);
            PageFilter pageFilter = new PageFilter(WORK_SIZE);
            filters.add(pageFilter);
            FilterList filterList = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            scan.setStartRow(startRow);
            try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
                HTable table = (HTable) connection.getTable(TableName.valueOf(Conf.ACCOUNT_TABLE));
                ResultScanner resultScanner = table.getScanner(scan);
                for (Result scannerResult : resultScanner) {
                    byte[] row = scannerResult.getRow();
                    RowTransaction rowTransaction = new RowTransaction(Conf.ACCOUNT_TABLE, Bytes.toString(row) + Transaction.NOTIFICATION_FAMILY_FLAG);
                    rowTransaction.startRowTransaction();
                    if (table.exists(new Get(row).addColumn(Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), Bytes.toBytes(Transaction.NOTIFICATION_FAMILY_FLAG)))) {
                        boolean observerRunSuccess = observerRun(row);
                        if (observerRunSuccess) {
                            Delete deleteFlag = new Delete(row).addColumn(Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), Bytes.toBytes(Transaction.NOTIFICATION_FAMILY_FLAG));
                            table.delete(deleteFlag);
                        } else {
                            logger.warn("fail to run observer! for row = {}", Bytes.toString(row));
                        }
                    }
                    rowTransaction.commit();
                }
            } catch (Exception e) {
                logger.error(Throwables.getStackTraceAsString(e));
            }
        }

        private boolean observerRun(byte[] row) throws IOException {
            Transaction accountTransaction = new Transaction(Conf.ACCOUNT_TABLE);
            long currentValue = accountTransaction.get(Bytes.toString(row), Transaction.ACCOUNT_FAMILY);
            accountTransaction.commit();
            Transaction recordTransaction = new Transaction(Conf.RECORD_TABLE);
            Long recordValue = recordTransaction.get(Bytes.toString(row), RECORD_TABLE_FAMILY);
            Long upCount = recordTransaction.get(UP_COUNT_KEY, RECORD_TABLE_FAMILY);
            Long downCount = recordTransaction.get(DOWN_COUNT_KEY, RECORD_TABLE_FAMILY);
            if (recordValue == null) {
                Write flagWrite = new Write(Bytes.toString(row), RECORD_TABLE_FAMILY, 1l);
                recordTransaction.addWrite(flagWrite);
                Write countWrite = new Write(UP_COUNT_KEY, RECORD_TABLE_FAMILY, upCount + 1);
                recordTransaction.addWrite(countWrite);
            } else if (currentValue >= Conf.RECORD_THRESHOLD && recordValue <= 0l) {
                Write flagWrite = new Write(Bytes.toString(row), RECORD_TABLE_FAMILY, 1l);
                recordTransaction.addWrite(flagWrite);
                Write upCountWrite = new Write(UP_COUNT_KEY, RECORD_TABLE_FAMILY, upCount + 1);
                recordTransaction.addWrite(upCountWrite);
                //down -1
                Write downCountWrite = new Write(DOWN_COUNT_KEY, RECORD_TABLE_FAMILY, downCount - 1);
                recordTransaction.addWrite(downCountWrite);
            } else if (currentValue < Conf.RECORD_THRESHOLD && recordValue > 0l) {
                Write flagWrite = new Write(Bytes.toString(row), RECORD_TABLE_FAMILY, 0l);
                recordTransaction.addWrite(flagWrite);
                Write upCountWrite = new Write(UP_COUNT_KEY, RECORD_TABLE_FAMILY, upCount - 1);
                recordTransaction.addWrite(upCountWrite);
                //down -1
                Write downCountWrite = new Write(DOWN_COUNT_KEY, RECORD_TABLE_FAMILY, downCount + 1);
                recordTransaction.addWrite(downCountWrite);
            } else {
                logger.info("no operation for record table");
            }


            return recordTransaction.commit();
        }


        private byte[] getRandomRow() {
            byte[] selectedRow = null;
            try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
                HTable accountTable = (HTable) connection.getTable(TableName.valueOf(Conf.ACCOUNT_TABLE));
                //filter by notification column
                List<Filter> filters = new ArrayList<>();
                //value filter
                Filter singleColumnValueFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), Bytes.toBytes(Transaction.NOTIFICATION_FAMILY_FLAG), CompareFilter.CompareOp.EQUAL, new byte[]{1});
                filters.add(singleColumnValueFilter);
                RandomRowFilter randomRowFilter = new RandomRowFilter(RANDOM_FILTER_CHANCE);
                filters.add(randomRowFilter);
                PageFilter pageFilter = new PageFilter(RANDOM_PAGE_SIZE);
                filters.add(pageFilter);
                //random filter
                Filter filterList = new FilterList(filters);
                Scan accountTableScan = new Scan();
                accountTableScan.setFilter(filterList);
                ResultScanner randomScanner = accountTable.getScanner(accountTableScan);
                List<byte[]> randomRowList = new ArrayList<>();
                try {
                    for (Result randomScannerResult : randomScanner) {
                        byte[] row = randomScannerResult.getRow();
                        randomRowList.add(row);
                    }
                    if (!randomRowList.isEmpty()) {
                        Random rand = new Random();
                        int index = (int) (randomRowList.size() * rand.nextDouble());
                        selectedRow = randomRowList.get(index);
                    }
                } finally {
                    randomScanner.close();
                }


            } catch (IOException e) {
                logger.error(Throwables.getStackTraceAsString(e));
            }
            return selectedRow;
        }


    }
}
