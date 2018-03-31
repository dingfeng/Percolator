package suportservice;


import suportservice.service.SupportServer;

import java.rmi.Naming;

/**
 * @author 丁峰
 * @date 2018/3/28 19:56
 * @see client
 */
public class client {
    public static void main(String args[]) {
        try {
            SupportServer server = (SupportServer) Naming.lookup("rmi://127.0.0.1:20000/SupportServer");
            System.out.println(server.getTimestamps(3));
            System.out.println(server.isAlive(1));
        } catch (Exception e) {
            System.err.println("ComputePi exception:");
            e.printStackTrace();
        }
    }
}
