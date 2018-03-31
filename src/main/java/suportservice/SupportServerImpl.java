package suportservice;


import suportservice.service.SupportServer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 丁峰
 * @date 2018/3/28 19:19
 * @see SupportServerImpl
 */
public class SupportServerImpl extends UnicastRemoteObject implements SupportServer {
    private long timestamp = 0;
    private Map<Long, Long> aliveMap = new ConcurrentHashMap<>();
    private final static long maxNoAliveDuration = 60 * 1000;

    public SupportServerImpl() throws RemoteException {
        super();
    }

    @Override
    public synchronized List<Long> getTimestamps(int num) {
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < num; ++i) {
            timestamps.add(timestamp++);
        }
        return timestamps;
    }


    //get time value on the application host in milliseconds
    private long getDateTimestamp() {
        return Calendar.getInstance().getTimeInMillis();
    }

    @Override
    public void keepAlive(long id) {
        long current = getDateTimestamp();
        aliveMap.put(id, current);
    }

    @Override
    public boolean isAlive(long id) {
        Long lastAliveTime = aliveMap.get(id);
        //id not exist or time margin is larger than maxNoAliveDuration
        if (lastAliveTime == null || (getDateTimestamp() - lastAliveTime) > maxNoAliveDuration) {
            if (lastAliveTime != null) {
                aliveMap.remove(id);
            }
            return false;
        }
        return true;
    }


}
