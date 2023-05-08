package org.xenei.rdfstore.store;

public class Key implements Comparable<Key> {
    private Integer value;

    public Key(int value) {
        this.value = value;
    }

    public Key(Integer value) {
        this.value = value;
    }

    public Key(long value) {
        this.value = (int) value;
    }

    public long asUnsigned() {
        return Integer.toUnsignedLong(value);
    }

    public int asSigned() {
        return value.intValue();
    }

    @Override
    public int compareTo(Key other) {
        if (value == other.value) {
            return 0;
        }
        if (value == null) {
            return -1;
        }
        if (other.value == null) {
            return 1;
        }
        return Integer.compareUnsigned(this.value, other.value);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Key) {
            return asUnsigned() == ((Key)o).asUnsigned();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(asUnsigned());
    }
    @Override
    public String toString() {
        return String.format("Key[%s]", asUnsigned());
    }
}