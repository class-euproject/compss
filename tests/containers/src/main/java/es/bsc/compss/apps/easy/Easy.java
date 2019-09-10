package es.bsc.compss.apps.easy;

public class Easy {

    public static void main(String [] args) {
        if (args.length != 2) {
            System.out.println("Incorrect amount of arguments");
            System.exit(-1);
        }

        int max = Integer.parseInt(args[0]);
        int b = Integer.parseInt(args[1]);
        System.out.println("Max random number is " + max + " and b is " + b);
        int random = EasyImpl.getRandomNumber(max);
        System.out.println("Random number is " + random);
        try {
            for (int i = 0; i < 20; i++) {
                System.out.println(i);
                Thread.sleep(1000);
            }
        } catch (Throwable t) {}
        int sum = EasyImpl.sum(random, b);
        System.out.println(random + " + " + b + " = " + sum);
    }

}
