package ads.sjtu.edu.cn.Percolator.transaction;


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
        this.lockKey = new StringBuilder().append("/").append(table).append("/").append(key).toString();
    }

    public boolean startRowTransaction() {
        logger.info("start row transaction for lockKey={}", lockKey);
        boolean result = DistributeLockUtils.lock(lockKey);
        logger.info("get lock for {}",lockKey);
        return result;
    }
//0x1626823e9230022
    public boolean commit() {
        logger.info("commit row transaction for lockKey={}", lockKey);
        boolean result = DistributeLockUtils.unlock(lockKey);
        return result;
    }


}
