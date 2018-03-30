package ads.sjtu.edu.cn.Percolator.model;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private void backoffAndMaybeCleanuplock(HTable table, String row, String family) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COl)));
        long rowResultTimestamp = result.rawCells()[0].getTimestamp();
        if (result.getExists()) {
            String primaryLocation = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(LOCK_COl)));
            primaryLocation = primaryLocation.substring(1, primaryLocation.length() - 1);
            String[] primaryStrArray = primaryLocation.split(",");
            String primaryRow = primaryStrArray[0];
            String primaryFamily = primaryStrArray[1];
            long primaryCommitTimestamp = -1;
            RowTransaction rowTransaction = new RowTransaction(TABLE_NAME, primaryRow);
            logger.info("begin transaction primary row = {}", primaryRow);
            rowTransaction.startRowTransaction();
            //primary行是否存在锁
            Result primaryWriteResult = table.get(new Get(Bytes.toBytes(primaryRow)).addColumn(Bytes.toBytes(primaryFamily), Bytes.toBytes(WRITE_COL)).setTimeStamp(rowResultTimestamp));
            if (primaryWriteResult.getExists()) {
                //roll-forward set primary commit timestamp
                primaryCommitTimestamp = primaryWriteResult.rawCells()[0].getTimestamp();
                logger.info("roll forward primary commit timestamp = {} ", primaryCommitTimestamp);
            }

            rowTransaction.commit();
            if (primaryCommitTimestamp == -1) {
                //roll back delete data and lock
                RowMutations rowMutations = new RowMutations(Bytes.toBytes(row));
                Delete dataDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(DATA_COL)).setTimestamp(rowResultTimestamp);
                Delete lockDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COl)).setTimestamp(rowResultTimestamp);
                rowMutations.add(dataDelete);
                rowMutations.add(lockDelete);
                table.mutateRow(rowMutations);
                logger.info("succeed to roll back row = {}", row);
            } else {
                //roll-forward add a write
                RowMutations rowMutations = new RowMutations(Bytes.toBytes(row));
                Delete lockDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COl)).setTimestamp(rowResultTimestamp);
                Put writePut = new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(WRITE_COL), primaryCommitTimestamp, Bytes.toBytes(rowResultTimestamp));
                rowMutations.add(lockDelete);
                rowMutations.add(writePut);
                table.mutateRow(rowMutations);
                logger.info("succeed to roll forward row = {}", row);
            }
        }
    }

    public byte[] get(String row, String col) throws IOException {
        HTable table = (HTable) this.connection.getTable(TableName.valueOf(TABLE_NAME));
        byte[] rowBytes = Bytes.toBytes(row);
        byte[] familyBytes = Bytes.toBytes(col);
        while (true) {
            Get lockGet = new Get(rowBytes);
            lockGet.setTimeRange(0, startTimestamp);
            lockGet.addColumn(familyBytes, Bytes.toBytes(LOCK_COl));
            if (table.exists(lockGet)) {
                logger.info("row = {} has been locked", row);
                backoffAndMaybeCleanuplock(table, row, col);
                continue;
            }
            Result result = table.get(new Get(rowBytes).setTimeRange(0, startTimestamp).addColumn(familyBytes, Bytes.toBytes("write")));
            if (!result.getExists()) {
                logger.warn("no result for rowKey={} ", row);
                return null;
            }
            long lastest_write = Bytes.toLong(result.getValue(familyBytes, Bytes.toBytes("write")));
            Result dataResult = table.get(new Get(rowBytes).setTimeRange(lastest_write, lastest_write).addColumn(familyBytes, Bytes.toBytes("data")));
            byte[] dataBytes = dataResult.getValue(familyBytes, Bytes.toBytes("data"));
            return dataBytes;
        }
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
        RowMutations mutations = new RowMutations(Bytes.toBytes(row));
        mutations.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(DATA_COL), startTimestamp, Bytes.toBytes(w.getValue())));
        mutations.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(LOCK_COl), startTimestamp, Bytes.toBytes("{" + primary.getRow() + "," + primary.getCol() + "}")));
        table.mutateRow(mutations);
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
        if (!table.exists(new Get(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COl)).setTimeStamp(startTimestamp)))
            return false;
        RowMutations mutations = new RowMutations(Bytes.toBytes(primary.getRow()));
        mutations.add(new Put(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
        mutations.add(new Delete(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COl)));
        table.mutateRow(mutations);
        if (!rowTransaction.commit()) return false;
        logger.info("primary succeeds to commit row transaction");
        //update other rows
        for (int i = 1; i < writes.size(); ++i) {
            Write write = writes.get(i);
            RowMutations rowMutations = new RowMutations(Bytes.toBytes(write.getRow()));
            rowMutations.add(new Put(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
            rowMutations.add(new Delete(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(LOCK_COl)));
            table.mutateRow(rowMutations);
        }
        return true;
    }

}
