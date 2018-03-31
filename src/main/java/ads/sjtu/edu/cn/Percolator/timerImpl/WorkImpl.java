package ads.sjtu.edu.cn.Percolator.timerImpl;


import ads.sjtu.edu.cn.Percolator.component.ThreadPool;
import ads.sjtu.edu.cn.Percolator.timer.Worker;
import ads.sjtu.edu.cn.Percolator.transaction.Conf;
import ads.sjtu.edu.cn.Percolator.transaction.Transaction;
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


/**
 * @author 丁峰
 * @date 2018/3/31 13:27
 * @see WorkImpl
 */
@Component
public class WorkImpl implements Worker {
    static Logger logger = LoggerFactory.getLogger(WorkImpl.class);
    private static final String RECORD_TABLE = "record_table";
    private static final String ACCOUNT_TABLE = "account_table";
    private static final int WORK_SIZE = 1000;
    private static final float RANDOM_FILTER_CHANCE = 1.0f / WORK_SIZE;
    private static final int RANDOM_PAGE_SIZE = 100;
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
            try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
                HTable accountTable = (HTable) connection.getTable(TableName.valueOf(ACCOUNT_TABLE));
                //filter by notification column
                List<Filter> filters = new ArrayList<>();
                //value filter
                Filter singleColumnValueFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), Bytes.toBytes(Transaction.NOTIFICATION_FAMILY), CompareFilter.CompareOp.EQUAL, new byte[]{1});
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
                } finally {
                    randomScanner.close();
                }
            } catch (IOException e) {
                logger.error(Throwables.getStackTraceAsString(e));
            }

        }
    }
}
