package es.bsc.compss.apps.easy;

import java.util.Random;

public class EasyImpl {

    public static int getRandomNumber(int max) {
        return new Random().nextInt(max);
    }

    public static int sum(int a, int b) {
        return a + b;
    }

}
