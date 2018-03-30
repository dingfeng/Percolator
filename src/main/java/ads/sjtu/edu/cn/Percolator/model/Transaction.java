package ads.sjtu.edu.cn.Percolator.model;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 丁峰
 * @date 2018/3/28 20:53
 * @see Transaction
 */
public class Transaction {
    static Logger logger = LoggerFactory.getLogger(Transaction.class);
    public final static String TABLE_NAME = "account_table";
    public final static String LOCK_COl = "lock";
    public final static String WRITE_COL = "write";
    public final static String DATA_COL = "data";
    //    private SupportServiceClient supportServiceClient = SupportServiceClient.getInstance();
    private Connection connection;
    private long startTimestamp;
    private List<Write> writes = new ArrayList<>();

    public Transaction() throws IOException {
        this.connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);
        this.startTimestamp = getOneTimestamp();
    }

    private long getOneTimestamp() throws RemoteException {
        return SupportServiceClient.getInstance().getTimestamps(1).get(0);
    }


    public void addWrite(Write w) {
        writes.add(w);
    }

    private void backoffAndMaybeCleanuplock(String row, String family) {

    }

    public byte[] get(String row, String col) throws IOException {
        HTable table = (HTable) this.connection.getTable(TableName.valueOf(TABLE_NAME));
        byte[] rowBytes = Bytes.toBytes(row);
        byte[] familyBytes = Bytes.toBytes(col);
        while (true) {
            Get lockGet = new Get(rowBytes);
            lockGet.setTimeRange(0, startTimestamp);
            lockGet.addColumn(familyBytes, Bytes.toBytes("lock"));
            if (!table.exists(lockGet)) {
                backoffAndMaybeCleanuplock(row, col);
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

    public boolean pWrite(Write w, Write primary) throws IOException {
        String row = w.getRow();
        String col = w.getCol();
        RowTransaction rowTransaction = new RowTransaction(TABLE_NAME, row);
        rowTransaction.startRowTransaction();
        HTable table = (HTable) this.connection.getTable(TableName.valueOf(TABLE_NAME));
        boolean writeExist = table.exists(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(WRITE_COL)).setTimeRange(startTimestamp, Long.MAX_VALUE));
        if (writeExist) {
            return false;
        }
        boolean lockExist = table.exists(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(LOCK_COl)));
        if (lockExist) {
            return false;
        }
        table.put(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(DATA_COL), startTimestamp, Bytes.toBytes(w.getValue())));
        table.put(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(LOCK_COl), startTimestamp, Bytes.toBytes("{" + primary.getRow() + "," + primary.getCol() + "}")));
        return rowTransaction.commit();
    }


    public boolean commit() throws IOException {
        if (writes.isEmpty()) {
            logger.info("commit empty writes");
            return true;
        }
        Write primary = writes.get(0);
        if (!pWrite(primary, primary)) return false;
        for (int i = 1; i < writes.size(); ++i) {
            Write wirte = writes.get(i);
            if (!pWrite(wirte, primary)) return false;
        }
        long commitTimestamp = getOneTimestamp();
        logger.info("primary start row transaction");
        RowTransaction rowTransaction = new RowTransaction(TABLE_NAME, primary.getRow());
        rowTransaction.startRowTransaction();
        HTable table = (HTable) this.connection.getTable(TableName.valueOf(TABLE_NAME));
        if (!table.exists(new Get(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COl)).setTimeRange(startTimestamp, startTimestamp + 1)))
            return false;
        table.put(new Put(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
        table.delete(new Delete(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COl)));
        if (!rowTransaction.commit()) return false;
        logger.info("primary succeeds to commit row transaction");
        //update other rows
        for (int i = 1; i < writes.size(); ++i) {
            Write write = writes.get(i);
            table.put(new Put(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
            table.delete(new Delete(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(LOCK_COl)));
        }
        return true;
    }

}
