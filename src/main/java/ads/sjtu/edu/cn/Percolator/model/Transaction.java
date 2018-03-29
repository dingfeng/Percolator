package ads.sjtu.edu.cn.Percolator.model;


import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 丁峰
 * @date 2018/3/28 20:53
 * @see Transaction
 */
public class Transaction {
    Logger logger = LoggerFactory.getLogger(Transaction.class);
    public final static String TABLE_NAME = "account_table";
    public final static String LOCK_COl = "lock";
    public final static String WRITE_COL = "write";
    private SupportServiceClient supportServiceClient = SupportServiceClient.getInstance();
    private Connection connection;
    private long startTimestamp;
    private long commitTimestamp;

    public Transaction(String table) {
        try {
            this.connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        } catch (IOException e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
    }

    private List<Write> writes = new ArrayList<>();

    public void addWrite(Write w) {
        writes.add(w);
    }

    private void backoffAndMaybeCleanuplock(String row, String family) {

    }

    public byte[] get(String row, String family) throws IOException {
        HTable table = (HTable) this.connection.getTable(TableName.valueOf(TABLE_NAME));
        byte[] rowBytes = Bytes.toBytes(row);
        byte[] familyBytes = Bytes.toBytes(family);
        while (true) {
            Get lockGet = new Get(rowBytes);
            lockGet.setTimeRange(0, startTimestamp);
            lockGet.addColumn(familyBytes, Bytes.toBytes("lock"));
            if (!table.exists(lockGet)) {
                backoffAndMaybeCleanuplock(row, family);
                continue;
            }
            Result result = table.get(new Get(rowBytes).setTimeRange(0, startTimestamp).addColumn(familyBytes, Bytes.toBytes("write")));
            if (result == null) {
                return null;
            }
            long lastest_write = toLong(result.getValue(familyBytes, Bytes.toBytes("write")));
            Result dataResult = table.get(new Get(rowBytes).setTimeRange(lastest_write, lastest_write).addColumn(familyBytes, Bytes.toBytes("data")));
            byte[] dataBytes = dataResult.getValue(familyBytes, Bytes.toBytes("data"));
            return dataBytes;
        }
    }


    private long toLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public boolean pWrite(Write w, Write primary) {

        return false;
    }

    public boolean commit() {
        return false;
    }

}
