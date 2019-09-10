package es.bsc.compss.apps.overload;

public class OverloadImpl {

    public static void run() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
