package suportservice;


import suportservice.service.SupportServer;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

/**
 * @author 丁峰
 * @date 2018/3/28 19:49
 * @see main
 */

public class main {
    public static void main(String[] args) {
//        if (System.getSecurityManager() == null) {
//            System.setSecurityManager(new SecurityManager());
//        }
        try {
            long startTimestamp = 0;
            for (String timestamp : args) {
                if (timestamp.contains("st=")) {
                    startTimestamp=Long.parseLong(timestamp.substring(3));
                }
            }
            SupportServer server = new SupportServerImpl(startTimestamp);
            LocateRegistry.createRegistry(20000);
            Naming.bind("rmi://0.0.0.0:20000/SupportServer", server);
            System.out.println("SupportServer");
        } catch (Exception e) {
            System.err.println("SupportServer exception:");
            e.printStackTrace();
        }
    }
}
