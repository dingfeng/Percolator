package ads.sjtu.edu.cn.Percolator.model;

/**
 * @author 丁峰
 * @date 2018/3/29 12:59
 * @see RowTransaction
 */
public class RowTransaction {
    private long timestamp;

    public RowTransaction(long timestamp) {
        this.timestamp = timestamp;
    }

    public void startRowTransaction(byte[] key) {

    }

    public boolean commit(){
      return false;
    }


}
