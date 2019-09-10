package es.bsc.compss.apps.matmul;

public class MatmulImpl {

    public static void multiplyAccumulative(String file1, String file2, int bsize) throws Exception {
        Block a = new Block(file1, bsize);
        Block b = new Block(file2, bsize);
        Block.initBlockFile("result", bsize, false);
        Block c = new Block("result", bsize);

        c.multiplyAccum(a, b);
        c.printBlock();
        c.blockToDisk("result");
    }

}
