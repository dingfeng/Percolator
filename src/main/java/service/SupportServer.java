package service;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by FD on 2018/3/28.
 */
public interface SupportServer extends Remote{
    List<Long> getTimestamps(int num) throws RemoteException;

    void keepAlive(int id) throws RemoteException;

    boolean isAlive(int id) throws RemoteException;
}
