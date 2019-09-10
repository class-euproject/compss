package es.bsc.compss.apps.simple;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class Simple {

    public static String fileName = "counter";

    public static void main(String [] args) throws Exception {

        if (args.length != 1) {
            System.out.println("One argument is needed.");
            throw new Exception("[ERROR]: One argument (an integer) is needed.");
        }

        int initialValue = Integer.parseInt(args[0]);
        FileOutputStream fos = new FileOutputStream(fileName);
        fos.write(initialValue);
        fos.close();
        System.out.println("Initial counter value is " + initialValue);

        SimpleImpl.increment(fileName);

        FileInputStream fis = new FileInputStream(fileName);
        int finalValue = fis.read();
        fis.close();
        System.out.println("Final counter value is " + finalValue);
    }

}
