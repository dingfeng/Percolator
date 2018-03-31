package ads.sjtu.edu.cn.Percolator.transaction;

import ads.sjtu.edu.cn.Percolator.Conf;
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
    public final static String LOCK_COL = "lock";
    public final static String WRITE_COL = "write";
    public final static String DATA_COL = "data";
    public final static String ACCOUNT_FAMILY = "account";
    public final static String NOTIFICATION_FAMILY = "notification";
    public final static String NOTIFICATION_FAMILY_FLAG = "flag";
    private long startTimestamp;
    private List<Write> writes = new ArrayList<>();
    private String tableName;

    public Transaction(String tableName) throws IOException {
        this.startTimestamp = getOneTimestamp();
        this.tableName = tableName;
    }

    public Transaction() throws IOException {
        this(Conf.ACCOUNT_TABLE);
    }


    private long getOneTimestamp() throws RemoteException {
        return SupportServiceClient.getInstance().getTimestamps(1).get(0);
    }


    public void addWrite(Write w) {
        writes.add(w);
    }

    private void backoffAndMaybeCleanuplock(HTable table, String row, String family) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COL)));
        long rowResultTimestamp = result.rawCells()[0].getTimestamp();
        boolean isAlive = SupportServiceClient.getInstance().isAlive(rowResultTimestamp);
        String primaryLocation = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(LOCK_COL)));
        primaryLocation = primaryLocation.substring(1, primaryLocation.length() - 1);
        String[] primaryStrArray = primaryLocation.split(",");
        String primaryRow = primaryStrArray[0];
        String primaryFamily = primaryStrArray[1];
        long primaryCommitTimestamp = -1;
        RowTransaction rowTransaction = new RowTransaction(this.tableName, primaryRow);
        logger.info("begin transaction primary row = {}", primaryRow);
        rowTransaction.startRowTransaction();
        //primary行是否存在锁
        Result primaryWriteResult = table.get(new Get(Bytes.toBytes(primaryRow)).addColumn(Bytes.toBytes(primaryFamily), Bytes.toBytes(WRITE_COL)).setTimeStamp(rowResultTimestamp));
        if (primaryWriteResult.rawCells() != null && primaryWriteResult.rawCells().length > 0) {
            primaryCommitTimestamp = primaryWriteResult.rawCells()[0].getTimestamp();
            logger.info("roll forward primary commit timestamp = {} ", primaryCommitTimestamp);
        }
        rowTransaction.commit();
        if (primaryCommitTimestamp == -1) {
            if (isAlive == false) {
                //roll back delete data and lock
                //delete lock of primary
                if (table.exists(new Get(Bytes.toBytes(primaryRow)).addColumn(Bytes.toBytes(primaryFamily), Bytes.toBytes(LOCK_COL)).setTimeStamp(rowResultTimestamp))) {
                    RowMutations primaryMutations = new RowMutations(Bytes.toBytes(primaryRow));
                    Delete primaryDataDelete = new Delete(Bytes.toBytes(primaryRow)).addColumn(Bytes.toBytes(family), Bytes.toBytes(DATA_COL)).setTimestamp(rowResultTimestamp);
                    Delete primaryLockDelete = new Delete(Bytes.toBytes(primaryRow)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COL)).setTimestamp(rowResultTimestamp);
                    primaryMutations.add(primaryDataDelete);
                    primaryMutations.add(primaryLockDelete);
                    table.mutateRow(primaryMutations);
                }
                //delete row
                RowMutations rowMutations = new RowMutations(Bytes.toBytes(row));
                Delete rowDataDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(DATA_COL)).setTimestamp(rowResultTimestamp);
                Delete rowLockDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COL)).setTimestamp(rowResultTimestamp);
                rowMutations.add(rowDataDelete);
                rowMutations.add(rowLockDelete);
                table.mutateRow(rowMutations);
                logger.info("succeed to roll back row = {}", row);
            }
        } else {
            //roll-forward add a write
            RowMutations rowMutations = new RowMutations(Bytes.toBytes(row));
            Delete lockDelete = new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(LOCK_COL)).setTimestamp(rowResultTimestamp);
            Put writePut = new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family), Bytes.toBytes(WRITE_COL), primaryCommitTimestamp, Bytes.toBytes(rowResultTimestamp));
            Put notificationPut = new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(NOTIFICATION_FAMILY), Bytes.toBytes(NOTIFICATION_FAMILY_FLAG), primaryCommitTimestamp, new byte[]{1});
            rowMutations.add(lockDelete);
            rowMutations.add(writePut);
            rowMutations.add(notificationPut);
            table.mutateRow(rowMutations);
            logger.info("succeed to roll forward row = {}", row);
        }
    }

    public Long get(String row, String col) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG);) {
            HTable table = (HTable) connection.getTable(TableName.valueOf(this.tableName));
            byte[] rowBytes = Bytes.toBytes(row);
            byte[] familyBytes = Bytes.toBytes(col);
            while (true) {
                Get lockGet = new Get(rowBytes);
                lockGet.setTimeRange(0, startTimestamp);
                lockGet.addColumn(familyBytes, Bytes.toBytes(LOCK_COL));
                if (table.exists(lockGet)) {
                    logger.info("row = {} has been locked", row);
                    backoffAndMaybeCleanuplock(table, row, col);
                    continue;
                }
                Result result = table.get(new Get(rowBytes).setTimeRange(0, startTimestamp).addColumn(familyBytes, Bytes.toBytes("write")));
                if (result.rawCells() == null || result.rawCells().length == 0) {
                    logger.warn("no result for rowKey={} ", row);
                    return null;
                }
                long lastest_write = Bytes.toLong(result.getValue(familyBytes, Bytes.toBytes("write")));
                Result dataResult = table.get(new Get(rowBytes).setTimeStamp(lastest_write).addColumn(familyBytes, Bytes.toBytes("data")));
                byte[] dataBytes = dataResult.getValue(familyBytes, Bytes.toBytes("data"));
                return Bytes.toLong(dataBytes);
            }
        } catch (IOException e) {
            throw e;
        }
    }


    private boolean pWrite(Connection connection, Write w, Write primary) throws IOException {

        String row = w.getRow();
        String col = w.getCol();
        RowTransaction rowTransaction = new RowTransaction(this.tableName, row);
        rowTransaction.startRowTransaction();
        HTable table = (HTable) connection.getTable(TableName.valueOf(this.tableName));
        boolean writeExist = table.exists(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(WRITE_COL)).setTimeRange(startTimestamp, Long.MAX_VALUE));
        if (writeExist) {
            return false;
        }
        boolean lockExist = table.exists(new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(LOCK_COL)));
        if (lockExist) {
            return false;
        }
        RowMutations mutations = new RowMutations(Bytes.toBytes(row));
        mutations.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(DATA_COL), startTimestamp, Bytes.toBytes(w.getValue())));
        mutations.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(col), Bytes.toBytes(LOCK_COL), startTimestamp, Bytes.toBytes("{" + primary.getRow() + "," + primary.getCol() + "}")));
        table.mutateRow(mutations);
        return rowTransaction.commit();
    }


    public boolean commit() throws IOException {
        if (writes.isEmpty()) {
            logger.info("commit empty writes");
            return true;
        }
        try (Connection connection = ConnectionFactory.createConnection(Conf.HBASE_CONFIG)) {
            Write primary = writes.get(0);
            if (!pWrite(connection, primary, primary)) return false;
            for (int i = 1; i < writes.size(); ++i) {
                Write write = writes.get(i);
                if (!pWrite(connection, write, primary)) return false;
            }
            long commitTimestamp = getOneTimestamp();
            logger.info("primary start row transaction");
            RowTransaction rowTransaction = new RowTransaction(this.tableName, primary.getRow());
            rowTransaction.startRowTransaction();
            HTable table = (HTable) connection.getTable(TableName.valueOf(this.tableName));
            if (!table.exists(new Get(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COL)).setTimeStamp(startTimestamp)))
                return false;
            RowMutations mutations = new RowMutations(Bytes.toBytes(primary.getRow()));
            mutations.add(new Put(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
            mutations.add(new Delete(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(primary.getCol()), Bytes.toBytes(LOCK_COL)));
            mutations.add(new Put(Bytes.toBytes(primary.getRow())).addColumn(Bytes.toBytes(NOTIFICATION_FAMILY), Bytes.toBytes(NOTIFICATION_FAMILY_FLAG), commitTimestamp, new byte[]{1}));

            table.mutateRow(mutations);
            if (!rowTransaction.commit()) return false;
            logger.info("primary succeeds to commit row transaction");
            //update other rows
            for (int i = 1; i < writes.size(); ++i) {
                Write write = writes.get(i);
                RowMutations rowMutations = new RowMutations(Bytes.toBytes(write.getRow()));
                rowMutations.add(new Put(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(WRITE_COL), commitTimestamp, Bytes.toBytes(startTimestamp)));
                rowMutations.add(new Delete(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(write.getCol()), Bytes.toBytes(LOCK_COL)));
                rowMutations.add(new Put(Bytes.toBytes(write.getRow())).addColumn(Bytes.toBytes(NOTIFICATION_FAMILY), Bytes.toBytes(NOTIFICATION_FAMILY_FLAG), commitTimestamp, new byte[]{1}));
                table.mutateRow(rowMutations);
            }
        } catch (IOException e) {
            throw e;
        }
        logger.info("transaction commited");
        return true;
    }

}
