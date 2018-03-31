package ads.sjtu.edu.cn.Percolator.entity;

import lombok.Data;

/**
 * @author 丁峰
 * @date 2018/3/31 22:02
 * @see AccountLine
 */
@Data
public class AccountLine {
    private Long version;
    private String data="";
    private String write="";
    private String lock="";
    private String flag="";
}
