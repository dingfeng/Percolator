package ads.sjtu.edu.cn.Percolator.model;


import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 丁峰
 * @date 2018/3/29 12:59
 * @see RowTransaction
 */
public class RowTransaction {
    static Logger logger = LoggerFactory.getLogger(RowTransaction.class);

    private String lockKey;

    public RowTransaction(String table, String key) {
        this.lockKey = new StringBuilder().append(table).append("/").append(key).toString();
    }

    public boolean startRowTransaction() {
        logger.info("start row transaction for lockKey={}", lockKey);
        return DistributeLockUtils.lock(lockKey);
    }

    public boolean commit() {
        logger.info("commit row transaction for lockKey={}", lockKey);
        return DistributeLockUtils.unlock(lockKey);
    }


}
