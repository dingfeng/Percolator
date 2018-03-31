package ads.sjtu.edu.cn.Percolator.timerimpl;


import ads.sjtu.edu.cn.Percolator.timer.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @Scheduled(cron = "* * * * * *")
    public void scanNotificationColumn() {
        logger.info("time triggered");
    }
}
