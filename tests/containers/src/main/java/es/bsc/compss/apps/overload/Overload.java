package es.bsc.compss.apps.overload;

public class Overload {

    public static int MAX = 1000;

    public static void main(String [] args) {
        for (int i = 0; i < MAX; i++) {
            OverloadImpl.run();
            OverloadImpl.run();
            OverloadImpl.run();
        }
    }

}
