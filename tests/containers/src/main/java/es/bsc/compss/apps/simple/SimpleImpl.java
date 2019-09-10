package es.bsc.compss.apps.simple;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SimpleImpl {

    public static void increment(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        int count = fis.read();
        fis.close();

        System.out.println("Counter is now" + ++count);

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(count);
        fos.close();
    }

}
