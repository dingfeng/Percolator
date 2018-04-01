package ads.sjtu.edu.cn.Percolator.controller;

import ads.sjtu.edu.cn.Percolator.controllerImpl.TransferControllerImpl;
import ads.sjtu.edu.cn.Percolator.entity.AccountData;
import ads.sjtu.edu.cn.Percolator.entity.Record;
import ads.sjtu.edu.cn.Percolator.entity.RowAccountVersionData;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountParam;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 丁峰
 * @date 2018/3/31 21:36
 * @see TransferController
 */
public interface TransferController {
    /**
     * 转账
     * @param transferAccountParam 转出账户,转入账户和转入数量
     * @return 是否转账成功
     */
    String transferAccount(TransferAccountParam transferAccountParam);

    /**
     *
     * @return 所有用户的账户数据：账户名称和金额
     */
    List<AccountData> allAccountData();

    /**
     * 包括版本的数据，包括版本
     * @return
     */
    List<RowAccountVersionData> allDBDataWithVersions();

    /**
     *
     * @return 返回记录，超过某金额的数量和低于某金额的数量
     */
    Record recordData();

}
