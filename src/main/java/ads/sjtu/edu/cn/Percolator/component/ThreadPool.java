package ads.sjtu.edu.cn.Percolator.component;

/**
 * @author 丁峰
 * @date 2018/3/31 13:35
 * @see ThreadPool
 */
public interface ThreadPool {
    void startTask(Runnable runnable);
}
