package ads.sjtu.edu.cn.Percolator.entity;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 丁峰
 * @date 2018/3/31 22:06
 * @see RowAccountVersionData
 */
@Data
public class RowAccountVersionData {
    private String account = "";
    //    private List<AccountLine> versionData;
    private Map<String, Map<String, Map<Long, String>>> data;
}
