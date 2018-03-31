package ads.sjtu.edu.cn.Percolator.serviceImpl;

import ads.sjtu.edu.cn.Percolator.Conf;
import ads.sjtu.edu.cn.Percolator.entity.Record;
import ads.sjtu.edu.cn.Percolator.service.RecordService;
import ads.sjtu.edu.cn.Percolator.timerImpl.WorkerImpl;
import ads.sjtu.edu.cn.Percolator.transaction.Transaction;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author 丁峰
 * @date 2018/3/31 22:26
 * @see RecordServiceImpl
 */
@Service
public class RecordServiceImpl implements RecordService {
    @Override
    public Record recordData() throws IOException {
        Transaction transaction = new Transaction(Conf.RECORD_TABLE);
        long upCount = transaction.get(WorkerImpl.UP_COUNT_KEY, "record");
        long downCount = transaction.get(WorkerImpl.DOWN_COUNT_KEY, "record");
        Record record = new Record();
        record.setDownCount(Long.toString(downCount));
        record.setUpCount(Long.toString(upCount));
        return record;
    }
}
