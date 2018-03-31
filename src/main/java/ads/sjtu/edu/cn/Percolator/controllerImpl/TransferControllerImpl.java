package ads.sjtu.edu.cn.Percolator.controllerImpl;

import ads.sjtu.edu.cn.Percolator.controller.TransferController;
import ads.sjtu.edu.cn.Percolator.entity.AccountData;
import ads.sjtu.edu.cn.Percolator.entity.AccountLine;
import ads.sjtu.edu.cn.Percolator.entity.Record;
import ads.sjtu.edu.cn.Percolator.entity.RowAccountVersionData;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 丁峰
 * @date 2018/3/31 21:36
 * @see TransferControllerImpl
 */
@Controller
@RequestMapping("/transfer")
public class TransferControllerImpl implements TransferController {
    static org.slf4j.Logger logger = LoggerFactory.getLogger(TransferControllerImpl.class);

    @RequestMapping("/transferAccount")
    @ResponseBody
    public String transferAccount(@RequestParam String account, @RequestParam List<TransferAccountItemParam> transferAccountItemParamList) {

        return "success";
    }

    @RequestMapping("/allAccountData")
    @ResponseBody
    public List<AccountData> allAccountData() {
        List<AccountData> accountDataList = new ArrayList<>();
        AccountData accountData = new AccountData();
        accountData.setAccount("chenbo");
        accountData.setAmount(1l);
        accountDataList.add(accountData);
        return accountDataList;
    }

    @RequestMapping("/allDBDataWithVersions")
    @ResponseBody
    public List<RowAccountVersionData> allDBDataWithVersions() {
        List<RowAccountVersionData> rowAccountVersionDataList = new ArrayList<>();
        RowAccountVersionData rowAccountVersionData = new RowAccountVersionData();
        rowAccountVersionData.setAccount("chebo");
        List<AccountLine> accountDataList = new ArrayList<>();
        AccountLine accountLine = new AccountLine();
        accountLine.setData("1");
        accountDataList.add(accountLine);
        rowAccountVersionData.setVersionData(accountDataList);
        rowAccountVersionDataList.add(rowAccountVersionData);
        return rowAccountVersionDataList;
    }

    @RequestMapping("/recordData")
    @ResponseBody
    public Record recordData()
    {
        return  new Record();
    }

}
