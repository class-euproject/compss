package es.bsc.compss.apps.xsimple;

public class Simple {

    public static String fileName = "counter";

    public static void main(String [] args) throws Exception {

        if (args.length != 1) {
            System.out.println("One argument is needed.");
            throw new Exception("[ERROR]: One argument (an integer) is needed.");
        }

        Integer initialValue = Integer.parseInt(args[0]);
        System.out.println("Initial counter value is " + initialValue);

        for (int i = 0; i < 4; i++) {
            SimpleImpl.increment(initialValue);
        }

        System.out.println("Now I'm gonna pretend like a read a file. aha ehem hmmm very interesting");
    }

}
