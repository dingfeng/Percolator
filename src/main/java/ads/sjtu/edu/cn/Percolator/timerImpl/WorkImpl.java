package ads.sjtu.edu.cn.Percolator.timerImpl;


import ads.sjtu.edu.cn.Percolator.component.ThreadPool;
import ads.sjtu.edu.cn.Percolator.timer.Worker;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


/**
 * @author 丁峰
 * @date 2018/3/31 13:27
 * @see WorkImpl
 */
@Component
public class WorkImpl implements Worker {
    static Logger logger = LoggerFactory.getLogger(WorkImpl.class);
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

        }
    }
}
