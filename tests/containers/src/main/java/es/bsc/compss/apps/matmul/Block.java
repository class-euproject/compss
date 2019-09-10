package es.bsc.compss.apps.matmul;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringTokenizer;

public class Block {

    private int blockSize;
    private double [][] data;

    public Block(String fileName, int blockSize) {
        this.blockSize = blockSize;
        data = new double [blockSize][blockSize];

        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            for (int i = 0; i < blockSize; i++) {
                StringTokenizer tokens = new StringTokenizer(lines.get(i));
                for (int j = 0; j < blockSize && tokens.hasMoreTokens(); j++) {
                    data[i][j] = Double.parseDouble(tokens.nextToken());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void multiplyAccum(Block a, Block b) {
        for (int i = 0; i < this.blockSize; i++) {
            for (int j = 0; j < this.blockSize; j++) {
                for (int k = 0; k < this.blockSize; k++) {
                    this.data[i][j] += a.data[i][k] * b.data[k][j];
                }
            }
        }
    }

    protected void printBlock() {
        for (int i = 0; i < this.blockSize; i++) {
            for (int j = 0; j < this.blockSize; j++) {
                System.out.print(data[i][j] + " ");
            }
            System.out.println();
        }
    }

    public void blockToDisk(String fileName) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName))) {
            for (int i = 0; i < this.blockSize; i++) {
                for (int j = 0; j < this.blockSize; j++) {
                    writer.write(data[i][j] + " ");
                }
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initBlockFile(String fileName, int blockSize, boolean initRand) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName))) {
            for (int i = 0; i < blockSize; i++) {
                for (int j = 0; j < blockSize; j++) {
                    double value = 0D;
                    if (initRand) value = Math.random()*10D;
                    writer.write(value + " ");
                }
                writer.newLine();
            }
            writer.newLine();
        }
    }

}
