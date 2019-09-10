package es.bsc.compss.apps.xsimple;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SimpleImpl {

    public static void increment(Integer n) throws IOException {
        System.out.println("Counter is now" + ++n);
    }

}
