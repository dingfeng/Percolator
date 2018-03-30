package ads.sjtu.edu.cn.Percolator.model;


import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 丁峰
 * @date 2018/3/28 20:51
 * @see Write
 */
@Data
@AllArgsConstructor
public class Write {
    private String row;
    private String col;
    private String value;
}
