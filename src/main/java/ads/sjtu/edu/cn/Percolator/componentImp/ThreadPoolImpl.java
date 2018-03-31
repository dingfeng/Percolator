package ads.sjtu.edu.cn.Percolator.componentImp;


import ads.sjtu.edu.cn.Percolator.component.ThreadPool;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 丁峰
 * @date 2018/3/31 13:36
 * @see ThreadPoolImpl
 */
@Component
public class ThreadPoolImpl implements ThreadPool {
    private ExecutorService executorService = Executors.newCachedThreadPool();

    @Override
    public void startTask(Runnable runnable) {
        executorService.submit(runnable);
    }
}
