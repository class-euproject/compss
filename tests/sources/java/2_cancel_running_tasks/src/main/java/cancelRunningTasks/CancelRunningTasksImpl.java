package cancelRunningTasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import es.bsc.compss.worker.COMPSsException;
import es.bsc.compss.worker.COMPSsWorker;


public class CancelRunningTasksImpl {

    public static void throwException(String fileName, Integer i) throws Exception {
        for (int j = 0; j <= i; j++) {
            Thread.sleep(j * 1000);
            System.out.println(j);
            // The exception is thrown from an specific task
            if (i == 1 && j == 1) {
                System.out.println("Exception is going to be thrown");
                throw new COMPSsException("Second task threw an exception");
            }
            COMPSsWorker.cancellationPoint();
        }
        System.out.println("Filename: " + fileName);
        writeFile(fileName, String.valueOf(1));
        System.out.println("1 written");
        String contents = readFile(fileName);
        System.out.println(contents);
    }

    public static void writeFile(String fileName, String i) {
        File f = new File(fileName);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(f, true));
            writer.write(i);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens
                writer.close();
            } catch (Exception e) {
            }
        }
    }

    public static void writeTwo(String fileName) {
        writeFile(fileName, String.valueOf(2));
        System.out.println("2 written");
        String contents = readFile(fileName);
        System.out.println(contents);
    }

    public static String readFile(String fileName) {
        File f = new File(fileName);
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents;
    }
}