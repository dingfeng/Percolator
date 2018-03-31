package ads.sjtu.edu.cn.Percolator.service;

import ads.sjtu.edu.cn.Percolator.entity.Record;

import java.io.IOException;

/**
 * Created by FD on 2018/3/31.
 */
public interface RecordService {
    Record recordData() throws IOException;
}
