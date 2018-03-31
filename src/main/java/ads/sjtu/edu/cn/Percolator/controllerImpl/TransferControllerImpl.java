package ads.sjtu.edu.cn.Percolator.controllerImpl;

import ads.sjtu.edu.cn.Percolator.controller.TransferController;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

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
        return null;
    }

    @RequestMapping("/allAccountData")
    @ResponseBody
    public String allAccountData() {
        return null;
    }

    @RequestMapping("/allDBDataWithVersions")
    public String allDBDataWithVersions() {
        return null;
    }


}
