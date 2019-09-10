package es.bsc.compss.apps.increment;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class IncrementImpl {

    public static void increment(String file) throws IOException {
        /*
        int count = Integer.parseInt(Files.readAllLines(Paths.get(file)).get(0));

        Files.write(Paths.get(file), Integer.toString(++count).getBytes());
        */

        FileInputStream fis = new FileInputStream(file);
        int count = fis.read();
        fis.close();

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(++count);
        fos.close();
    }

}
