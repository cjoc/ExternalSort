package uk.ac.cam.cjo41.fjava.tick0;

/**
 * Representation of a partition. Contains start position
 * and size of partition, to enable subsequent merging.
 * @author cjo41
 * All rights reserved.
 */
public class Partition {

    public final long start, size;

    public Partition(long start, long size) {
        this.start = start;
        this.size = size;
    }
    
    /**
     * Merges this partition with another, and returns a
     * new partition object as result.
     * @param p     Partition to merge.
     * @return      Merged partition.
     */
    public Partition merge(Partition p) {
        if (p.start < start) return new Partition(p.start, p.size + size);
        else return new Partition(start, p.size + size);
    }
}
