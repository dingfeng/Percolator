package ads.sjtu.edu.cn.Percolator.model;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author 丁峰
 * @date 2018/3/29 21:46
 * @see DistributeLockUtils
 */
public class DistributeLockUtils {
    static org.slf4j.Logger logger = LoggerFactory.getLogger(DistributeLockUtils.class);
    private static String connectString = "192.168.0.201:2181,192.168.0.202:2181,192.168.0.203:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
    private static Map<String, InterProcessSemaphoreMutex> lockMap = new ConcurrentHashMap<>();

    static {
        client.start();
    }

    public static boolean lock(String lockKey) {
        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, lockKey);
        try {
            lock.acquire();
            lockMap.put(lockKey, lock);
            return true;
        } catch (Exception e) {
            logger.error("lockKey={} ,get lock error: {}", lockKey, Throwables.getStackTraceAsString(e));
        }
        return false;
    }

    public static boolean unlock(String lockKey) {
        InterProcessSemaphoreMutex lock = lockMap.get(lockKey);
        if (lock != null) {
            try {
                lock.release();
                return true;
            } catch (Exception e) {
                logger.error("lockkey={}, unlock error : {}", lockKey, Throwables.getStackTraceAsString(e));
            }
        } else {
            logger.warn("lockKey={} does not exist", lockKey);
        }
        return false;
    }

}
