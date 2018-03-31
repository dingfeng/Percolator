package ads.sjtu.edu.cn.Percolator.serviceImpl;

import ads.sjtu.edu.cn.Percolator.Conf;
import ads.sjtu.edu.cn.Percolator.entity.AccountData;
import ads.sjtu.edu.cn.Percolator.entity.RowAccountVersionData;
import ads.sjtu.edu.cn.Percolator.param.TransferAccountItemParam;
import ads.sjtu.edu.cn.Percolator.service.AccountService;
import ads.sjtu.edu.cn.Percolator.transaction.SupportServiceClient;
import ads.sjtu.edu.cn.Percolator.transaction.Transaction;
import ads.sjtu.edu.cn.Percolator.transaction.Write;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * @author 丁峰
 * @date 2018/3/31 22:26
 * @see AccountServiceImpl
 */
@Service
public class AccountServiceImpl implements AccountService {
    @Override
    public boolean transferAccount(String account, List<TransferAccountItemParam> transferAccountItemParamList) throws IOException {
        Transaction transaction = new Transaction(Conf.ACCOUNT_TABLE);
        long accountData = transaction.get(account, Transaction.ACCOUNT_FAMILY);
        for (TransferAccountItemParam transferAccountItemParam : transferAccountItemParamList) {
            String inAccount = transferAccountItemParam.getAccount();
            long inAmount = transferAccountItemParam.getAmount();
            accountData -= inAmount;
            long inAccountData = transaction.get(inAccount, Transaction.ACCOUNT_FAMILY);
            inAccountData += inAmount;
            Write inAccountWrite = new Write(inAccount, Transaction.ACCOUNT_FAMILY, inAccountData);
            transaction.addWrite(inAccountWrite);
        }
        Write outAccountWrite = new Write(account, Transaction.ACCOUNT_FAMILY, accountData);
        transaction.addWrite(outAccountWrite);
        return transaction.commit();
    }

    @Override
    public List<AccountData> allAccountData() throws IOException {
        List<AccountData> accountDataList = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
            //get all keys
            HTable accountTable = (HTable) connection.getTable(TableName.valueOf(Conf.ACCOUNT_TABLE));
            long startTimestamp = SupportServiceClient.getInstance().getTimestamps(1).get(0);
            Scan scan = new Scan();
            scan.setTimeRange(0, startTimestamp);
            scan.setMaxVersions(1);
            scan.addColumn(Bytes.toBytes(Transaction.ACCOUNT_FAMILY), Bytes.toBytes(Transaction.WRITE_COL));
            ResultScanner resultScanner = accountTable.getScanner(scan);
            Set<String> allKeys = new HashSet<>();
            for (Result scannerResult : resultScanner) {
                byte[] row = scannerResult.getRow();
                if (row != null) {
                    String strKey = Bytes.toString(row);
                    allKeys.add(strKey);
                }
            }
            Transaction transaction = new Transaction(Conf.ACCOUNT_TABLE);
            for (String key : allKeys) {
                AccountData accountData = new AccountData();
                accountData.setAccount(key);
                long amount = transaction.get(key, Transaction.ACCOUNT_FAMILY);
                accountData.setAmount(amount);
                accountDataList.add(accountData);
            }
            transaction.commit();

        } catch (IOException e) {
            throw e;
        }
        return accountDataList;
    }

    @Override
    public List<RowAccountVersionData> allDBDataWithVersions() throws IOException {
        List<RowAccountVersionData> rowAccountVersionDataList = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
            HTable htable = (HTable) connection.getTable(TableName.valueOf(Conf.ACCOUNT_TABLE));
            Scan scan = new Scan();
            scan.setMaxVersions();
            ResultScanner resultScanner = htable.getScanner(scan);
            for (Result scannerResult : resultScanner) {
                RowAccountVersionData rowAccountVersionData = new RowAccountVersionData();
                byte[] row = scannerResult.getRow();
                rowAccountVersionData.setAccount(Bytes.toString(row));
                Map<String, Map<String, Map<Long, String>>> familyMap = new HashMap<>();
                rowAccountVersionData.setData(familyMap);
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = scannerResult.getMap();
                for (byte[] keySet : allVersions.keySet()) {
                    String keyStr = Bytes.toString(keySet);
                    Map<String, Map<Long, String>> colStrMap = new HashMap<>();
                    familyMap.put(keyStr, colStrMap);
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = allVersions.get(keySet);
                    for (byte[] colMapKey : colMap.keySet()) {
                        NavigableMap<Long, byte[]> colMapKeyMap = colMap.get(colMapKey);
                        String colMapKeyStr = Bytes.toString(colMapKey);
                        Map<Long, String> versionMap = new HashMap<>();
                        colStrMap.put(colMapKeyStr, versionMap);
                        for (Long versionKey : colMapKeyMap.keySet()) {
                            byte[] versionValue = colMapKeyMap.get(versionKey);
                            Long versionLongValue = Bytes.toLong(versionValue);
                            String value = null;
                            if (colMapKeyStr.equals("lock")) {
                                value = Bytes.toString(versionValue);
                            } else {
                                value = Long.toString(Bytes.toLong(versionValue));
                            }
                            versionMap.put(versionLongValue, value);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw e;
        }
        return rowAccountVersionDataList;
    }
}
