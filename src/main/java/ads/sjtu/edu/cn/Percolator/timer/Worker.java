package ads.sjtu.edu.cn.Percolator.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author 丁峰
 * @date 2018/3/31 12:59
 * @see Worker
 */
public interface Worker {
     void scanNotificationColumn();
}
