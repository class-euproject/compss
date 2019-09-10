package es.bsc.compss.apps.increment;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Increment {

    private static String baseFileName = "counter";

    private static void initializeCounter(int n, int value) throws Exception {
        try {
            FileOutputStream fos = new FileOutputStream(baseFileName + n);
            fos.write(value);
            fos.close();
        } catch (IOException e) {
            throw new Exception(e);
        }
    }

    private static void printCounters(int amount) throws Exception {
        for (int i = 0; i < amount; i++) {
            FileInputStream fis = new FileInputStream(baseFileName + i);
            int value = fis.read();
            fis.close();

            System.out.println(String.format("%d: %d", i, value));
        }
    }

    public static void main(String [] args) throws Exception {

        if (args.length < 2) {
            throw new Exception("[ERROR]: Incorrect number of parameters");
        }
         /*
        int N = Integer.parseInt(args[0]);
        List<Integer> counters = Arrays.stream(args).skip(1).map(Integer::parseInt).collect(Collectors.toList());
        for (int i = 0; i < counters.size(); i++) initializeCounter(i, counters.get(i));

        System.out.println("Initial values:");
        printCounters(counters.size());

        for (int i = 0; i < N; i++) for (int j = 0; j < counters.size(); j++) {
            IncrementImpl.increment(baseFileName + j);
        }

        System.out.println("Final values:");
        printCounters(counters.size());
        */

         int N = Integer.parseInt(args[0]);
         int counter1 = Integer.parseInt(args[1]);
         int counter2 = Integer.parseInt(args[2]);
         int counter3 = Integer.parseInt(args[3]);

         initializeCounter(0, counter1);
         initializeCounter(1, counter2);
         initializeCounter(2, counter3);

         System.out.println("Initial counter values: ");
         printCounters(3);

         for (int i=0; i<N; i++) {
             IncrementImpl.increment(baseFileName + 0);
             IncrementImpl.increment(baseFileName + 1);
             IncrementImpl.increment(baseFileName + 2);
         }

         System.out.println("Final counter values: ");
         printCounters(3);
    }

}
