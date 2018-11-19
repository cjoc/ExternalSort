package uk.ac.cam.cjo41.fjava.tick0;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Merges two partitions, as defined by construction parameters.
 * To be used on large sublists only.
 * @author cjo41
 * All rights reserved.
 */
public class SingleMergeThread extends Thread {

    private long start, toRead1, toRead2;
    private String from, to;
    
    /**
     * Constructor. Initialises thread, ready for execution.
     * @param p1        First partition. Must be adjacent to p2.
     * @param p2        Second partition. Must be adjacent to p1.
     * @param f1        Path to first file (A).
     * @param f2        Path to second file (B).
     * @param dir       Direction in which to perform sort.
     */
    public SingleMergeThread(Partition p1, Partition p2, String f1, String f2, SortDirection dir) {
        boolean p1First = p1.start < p2.start;
        this.start = p1First ? p1.start : p2.start;
        toRead1 = p1First ? p1.size : p2.size;
        toRead2 = p1First ? p2.size : p1.size;
        if (dir == SortDirection.A_TO_B) {
            this.from = f1;
            this.to = f2;
        }
        else {
            this.from = f2;
            this.to = f1;
        }
    }
    
    /**
     * Executes thread, and performs merging.
     */
    public void run() {
        try {
            // Creating RandomAccessFile instances.
            RandomAccessFile a1 = new RandomAccessFile(from,"r");
            RandomAccessFile a2 = new RandomAccessFile(from,"r");
            RandomAccessFile b = new RandomAccessFile(to,"rw");
            a1.seek(4*start);
            a2.seek(4*start + 4*toRead1);
            b.seek(4*start);

            // Creating streams to write data to "to".
            DataInputStream in1 = new DataInputStream(new BufferedInputStream(new FileInputStream(a1.getFD())));
            DataInputStream in2 = new DataInputStream(new BufferedInputStream(new FileInputStream(a2.getFD())));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(b.getFD())));
            
            // Getting the head element of each sublist, and decrementing toRead values.
            int front1 = in1.readInt();
            int front2 = in2.readInt();
            long toWrite1 = toRead1;
            long toWrite2 = toRead2;

            while (toWrite1 > 0 && toWrite2 > 0) {
                if (front1 < front2) {
                    // Write front1 to file.
                    out.writeInt(front1);
                    toWrite1--;
                    // Read next element in first sublist, if not read all.
                    if (toWrite1 > 0) front1 = in1.readInt();
                }
                else if (front1 == front2) {
                    // Write front1 to file.
                    out.writeInt(front1);
                    out.writeInt(front2);
                    toWrite1--;
                    toWrite2--;
                    // Read next element in first sublist, if not read all.
                    if (toWrite1 > 0) front1 = in1.readInt();
                    if (toWrite2 > 0) front2 = in2.readInt();
                }
                else {
                    // Write front2 to file.
                    out.writeInt(front2);
                    toWrite2--;
                    // Read next element in first sublist, if not read all.
                    if (toWrite2 > 0) front2 = in2.readInt();
                }
            }
            while (toWrite1 > 0) {
                out.writeInt(front1);
                toWrite1--;
                if (toWrite1 > 0) front1 = in1.readInt();
            }
            while (toWrite2 > 0) {
                out.writeInt(front2);
                toWrite2--;
                if (toWrite2 > 0) front2 = in2.readInt();
            }
            out.flush();
            a1.close();
            a2.close();
            b.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
