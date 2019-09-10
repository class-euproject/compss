package es.bsc.compss.apps.matmul;

public class Matmul {
    private static int BSIZE;

    private static String fileName1;
    private static String fileName2;

    public static void main(String [] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("ERROR: Incorrect number of parameters");
        }

        fileName1 = args[0];
        fileName2 = args[1];
        BSIZE = Integer.parseInt(args[2]);

        Block.initBlockFile(fileName1, BSIZE, true);
        Block.initBlockFile(fileName2, BSIZE, true);
        Block.initBlockFile("result", BSIZE, false);

        computeMultiplication();
    }

    private static void computeMultiplication() throws Exception {
        System.out.println("[LOG] Computing result:");
        MatmulImpl.multiplyAccumulative(fileName1, fileName2, BSIZE);
    }

}
