package ads.sjtu.edu.cn.Percolator.service;

import ads.sjtu.edu.cn.Percolator.entity.AccountData;
import ads.sjtu.edu.cn.Percolator.entity.RowAccountVersionData;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;

import java.io.IOException;
import java.util.List;

/**
 * Created by FD on 2018/3/31.
 */
public interface AccountService {
    boolean transferAccount(String account, List<TransferAccountItemParam> transferAccountItemParamList) throws IOException;

    List<AccountData> allAccountData() throws IOException;

    List<RowAccountVersionData> allDBDataWithVersions() throws IOException;
}
