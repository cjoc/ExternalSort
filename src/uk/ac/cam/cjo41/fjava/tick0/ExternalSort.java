package uk.ac.cam.cjo41.fjava.tick0;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main execution class.
 * @author cjo41
 * All rights reserved.
 */
public class ExternalSort {
    
    /**
     * External sort method. After execution, the integers stored
     * in f1 as bytes will be sorted with respect to each other.
     * @param f1        String path to file containing integers to sort.
     * @param f2        String path to auxiliary file, contents unimportant.
     */
    public static void sort(String f1, String f2) throws FileNotFoundException, IOException {

        // STAGE 1 - PARTITIONING
        // ----------------------------------------------------------------------------------
        // Split f1 into roughly equal partitions, each of which can be read into memory and
        // sorted individually. Number of partitions is of form 2^n for some n, to enable
        // simplified merging.
        
        // Getting file information and computing number of partitions required.
        long spaceInMemory = Runtime.getRuntime().freeMemory() / 8L;
        RandomAccessFile a = new RandomAccessFile(f1,"r");
        RandomAccessFile b = new RandomAccessFile(f2,"rw");
        final long FILE_LENGTH = a.length();
        int numberOfPartitionsRaw = (int) (FILE_LENGTH / spaceInMemory);
        if (numberOfPartitionsRaw == 0) numberOfPartitionsRaw++;
        
        // Rounding up to nearest power of 2, to enable easy merging later.
        final int numberOfPartitions = numberOfPartitionsRaw != Integer.highestOneBit(numberOfPartitionsRaw) ?
                2 * Integer.highestOneBit(numberOfPartitionsRaw) : numberOfPartitionsRaw;
        final long intsPerPartition = FILE_LENGTH / (4*numberOfPartitions);
        final int leftOver = intsPerPartition != 0 ? (int) ((FILE_LENGTH/4) % intsPerPartition) : 0;

        // Now iterating through each partition and sorting.
        List<Partition> partitions = new ArrayList<>();
        for (long i=0; i < numberOfPartitions; i++) {
            int partitionSize = i == 0 ? (int) (intsPerPartition + leftOver) : (int) intsPerPartition;
            long start = i == 0 ? 0L : (i * intsPerPartition) + leftOver;
            partitions.add(new Partition(start, partitionSize));
            
            byte[] unpacked = new byte[partitionSize*4];
            a.read(unpacked);
            int[] packed = pack(unpacked);
            
            Arrays.sort(packed);
            b.write(unpack(packed));
        }

        // Assert: f2 now contains the sorted partitions.
        // Stage 1 has been completed.
        

        // STAGE 2 - MERGING
        // ----------------------------------------------------------------------------------
        // If there was more than one partition, the partitions must be merged. This is
        // achieved using the SingleMergeThread class.
        
        SortDirection dir = SortDirection.B_TO_A;
        while (partitions.size() != 1) {
            List<Thread> threads = new ArrayList<>();
            List<Partition> newPartitions = new ArrayList<>();
            for (int i=0; i < partitions.size()-1; i+=2) {
                // Merging the two next partitions.
                Partition p1 = partitions.get(i);
                Partition p2 = partitions.get(i+1);
                Thread mergeThread = new SingleMergeThread(p1, p2, f1, f2, dir);
                mergeThread.run();
                threads.add(mergeThread);
                newPartitions.add(p1.merge(p2));
            }
            // Join all threads to this.
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            partitions = newPartitions;
            // Reset sort direction.
            dir = dir == SortDirection.A_TO_B ? SortDirection.B_TO_A : SortDirection.A_TO_B;
        }
        a.close();
        b.close();
        
        
        // STAGE 3 - Copying
        // ----------------------------------------------------------------------------------
        // If appropriate, we must now ensure the sorted numbers are in f1. Namely, if the
        // last step left the sorted files in f2, we must copy them to f1.

        if (dir == SortDirection.B_TO_A) {
            Path aPath = Paths.get(f1);
            Path bPath = Paths.get(f2);
            Files.copy(bPath, aPath, StandardCopyOption.REPLACE_EXISTING);
        }
        
        // Assert: f1 now contains the integers which were originally in
        // f1, but now in ascending order of magnitude.
        // The postcondition has been fulfilled.
    }

    /**
     * Takes a byte array, and returns the corresponding int array.
     * @param unpacked      Byte array containing data.
     * @return              Int array corresponding to input data.
     */
    private static int[] pack(byte[] unpacked) {
        IntBuffer intBuf = ByteBuffer.wrap(unpacked)
                                    .order(ByteOrder.BIG_ENDIAN)
                                    .asIntBuffer();
        int[] array = new int[intBuf.remaining()];
        intBuf.get(array);
        return array;
    }

    /**
     * Takes an int array, and returns the corresponding byte array.
     * @param packed        Int array containing data.
     * @return              Byte array corresponding to input data.
     */
    private static byte[] unpack(int[] packed) {
        ByteBuffer byteBuf = ByteBuffer.allocate(packed.length*4);
        byteBuf.asIntBuffer().put(packed);
        return byteBuf.array();
    }
    
    /**
     * Helper method for checksum. Converts byte to hex string.
     * @param b     Byte to convert.
     * @return      Corresponding hex string.
     */
    private static String byteToHex(byte b) {
        String r = Integer.toHexString(b);
        if (r.length() == 8) {
            return r.substring(6);
        }
        return r;
    }
    
    /**
     * Computes checksum for file whose path is given.
     * @param f     Path to file for which to compute checksum.
     * @return      Checksum for that file.
     */
    public static String checkSum(String f) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            DigestInputStream ds = new DigestInputStream(
                    new FileInputStream(f), md);
            byte[] b = new byte[512];
            while (ds.read(b) != -1) ;
            String computed = "";
            for (byte v : md.digest())
                computed += byteToHex(v);
            return computed;
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return "<error computing checksum>";
    }

    public static void main(String[] args) throws Exception {
        String f1 = args[0];
        String f2 = args[1];
        sort(f1, f2);
        System.out.println("The checksum is: "+checkSum(f1));
    }
}
