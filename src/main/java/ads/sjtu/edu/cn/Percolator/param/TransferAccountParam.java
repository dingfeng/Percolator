package ads.sjtu.edu.cn.Percolator.param;

import lombok.Data;

import java.util.List;

/**
 * @author 丁峰
 * @date 2018/4/1 10:35
 * @see TransferAccountParam
 */
@Data
public class TransferAccountParam {
    private String name;
    private  List<TransferAccountItemParam> transferAccountItemParamList;
}
