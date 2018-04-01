package ads.sjtu.edu.cn.Percolator.controllerImpl;

import ads.sjtu.edu.cn.Percolator.controller.TransferController;
import ads.sjtu.edu.cn.Percolator.entity.AccountData;
import ads.sjtu.edu.cn.Percolator.entity.Record;
import ads.sjtu.edu.cn.Percolator.entity.RowAccountVersionData;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountParam;
import ads.sjtu.edu.cn.Percolator.service.AccountService;
import ads.sjtu.edu.cn.Percolator.service.RecordService;
import com.google.common.base.Throwables;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
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
    @Autowired
    AccountService accountService;
    @Autowired
    RecordService recordService;

    @RequestMapping("/transferAccount")
    @ResponseBody
    public String transferAccount(@RequestBody TransferAccountParam transferAccountParam) {
        String name = transferAccountParam.getName();
        List<TransferAccountItemParam> transferAccountItemParamList = transferAccountParam.getTransferAccountItemParamList();
        String result = "failure";
        try {
            boolean transferResult = accountService.transferAccount(name, transferAccountItemParamList);
            if (transferResult) {
                result = "success";
            }
        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        return result;
    }

    @RequestMapping("/allAccountData")
    @ResponseBody
    public List<AccountData> allAccountData() {
        List<AccountData> resultList = Collections.emptyList();
        try {
            resultList = accountService.allAccountData();
        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        return resultList;
    }

    @RequestMapping("/allDBDataWithVersions")
    @ResponseBody
    public List<RowAccountVersionData> allDBDataWithVersions() {
        List<RowAccountVersionData> resultList = Collections.emptyList();
        try {
            resultList = accountService.allDBDataWithVersions();
        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        return resultList;
    }

    @RequestMapping("/recordData")
    @ResponseBody
    public Record recordData() {
        Record record = new Record();
        try {
            record = recordService.recordData();
        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        return record;
    }

}
