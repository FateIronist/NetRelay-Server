package top.fateironist.net_relay.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ConcurrentBitMap {
    private final int capacity;
    private final AtomicIntegerArray bitmap;
    private final AtomicInteger cardinality;

    public ConcurrentBitMap(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity can not less than 1");
        }
        this.capacity = capacity;
        this.bitmap = new AtomicIntegerArray((capacity % 32) == 0 ? (capacity / 32) : (capacity / 32 + 1));
        this.cardinality = new AtomicInteger(0);
    }

    public int getCapacity() {
        return capacity;
    }

    public int getCardinality() {
        return cardinality.get();
    }

    /**
     * set true
     * @param index
     * @return
     */
    public boolean set(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index out of bound(" + 0 + "~" +  (capacity - 1) + ")");
        }
        int intIndex = (index / 32);
        int bitIndex = (index % 32);
        int value = bitmap.get(intIndex);
        int mask = (1 << bitIndex);
        boolean ov = (value & mask) != 0;
        if (bitmap.compareAndSet(intIndex, value, value | mask)) {
            if (!ov) {
                cardinality.incrementAndGet();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * set false
     * @param index
     * @return
     */
    public boolean del(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index out of bound(" + 0 + "~" +  (capacity - 1) + ")");
        }
        int intIndex = (index / 32);
        int bitIndex = (index % 32);
        int value = bitmap.get(intIndex);
        int mask = (1 << bitIndex);
        boolean ov = (value & mask) != 0;
        if (bitmap.compareAndSet(intIndex, value, value & ~mask)) {
            if (ov) {
                cardinality.decrementAndGet();
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean get(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index " + index + " out of bound(" + 0 + "~" +  (capacity - 1) + ")");
        }
        int intIndex = (index / 32);
        int bitIndex = (index % 32);
        return (bitmap.get(intIndex) & (1 << bitIndex)) != 0;
    }

    /**
     * if success return value, else return null
     * @param index
     * @return
     */
    public Boolean getAndSet(int index, boolean v) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index " + index + " out of bound(" + 0 + "~" +  (capacity - 1) + ")");
        }
        int intIndex = (index / 32);
        int bitIndex = (index % 32);
        int value = bitmap.get(intIndex);
        int mask = (1 << bitIndex);
        boolean ov = (value & mask) != 0;
        int newValue = value;
        if (v) {
            newValue |= mask;
        } else {
            newValue &= ~mask;
        }
        if (bitmap.compareAndSet(intIndex, value, newValue)) {
            if ((v && !ov) || (!v && ov)) {
                if (v) {
                    cardinality.incrementAndGet();
                } else {
                    cardinality.decrementAndGet();
                }
            }
            return ov;
        } else {
            return null;
        }
    }

    public boolean setIfNotExist(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index " + index + " out of bound(" + 0 + "~" +  (capacity - 1) + ")");
        }
        int intIndex = (index / 32);
        int bitIndex = (index % 32);
        int value = bitmap.get(intIndex);

        int mask = (1 << bitIndex);
        if ((value & mask) == 0) {
            if (bitmap.compareAndSet(intIndex, value, value | mask)) {
                cardinality.incrementAndGet();
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
