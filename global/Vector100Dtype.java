package global;

import java.util.Iterator;

public class Vector100Dtype implements Iterable<Short> {

    private final short[] vector;

    public Vector100Dtype() {
        vector = new short[100];
    }

    public Vector100Dtype(short[] vector) {
        this.vector = vector;
    }

    public void set(int index,short value) throws IndexOutOfBoundsException, NullPointerException {
        try {
            vector[index] = value;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("ArrayIndexOutOfBoundsException at vector add");
            throw e;
        } catch (NullPointerException e) {
            System.err.println("NullPointerException at vector add");
            throw e;
        }
    }

    public void add(short value, int index) throws IndexOutOfBoundsException, NullPointerException {
        try {
            vector[index] = value;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("ArrayIndexOutOfBoundsException at vector add");
            throw e;
        } catch (NullPointerException e) {
            System.err.println("NullPointerException at vector add");
            throw e;
        }
    }

    public short get(int index) {
        try {
            return vector[index];
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("ArrayIndexOutOfBoundsException at vector get");
            throw e;
        } catch (NullPointerException e) {
            System.err.println("NullPointerException at vector get");
            throw e;
        }
    }

    @Override
    public Iterator<Short> iterator() {
        return new VectorIterator();
    }

    private class VectorIterator implements Iterator<Short> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < vector.length;
        }

        @Override
        public Short next() {
            return vector[index++];
        }
    }


    public double getDistance(Vector100Dtype other) {

        Iterator<Short> thisIterator = this.iterator();
        Iterator<Short> otherIterator = other.iterator();
        double sumSquares = 0.0;
        while (thisIterator.hasNext() && otherIterator.hasNext()) {
            sumSquares += Math.pow(thisIterator.next() - otherIterator.next(), 2);
        }
        return Math.sqrt(sumSquares);
    }

    public long distanceTo(Vector100Dtype other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot compute distance to null vector");
        }
        long sum = 0; // Use long to avoid overflow during summation
        for (int i = 0; i < 100; i++) {
            int diff = this.vector[i] - other.vector[i];
            sum = sum + (diff * diff);
        }
        long ed =  (long) Math.sqrt(sum); // Cast to int as per project spec
        return ed;
    }

    public short compareTo(Vector100Dtype other, int dist) {
        double calculatedDistance = this.getDistance(other);
        if (calculatedDistance < dist) {
            return -1;
        } else if (calculatedDistance > dist) {
            return 1;
        }
        return 0;
    }

    public short[] getVector() {
        return vector;
    }

    public Vector100Dtype(boolean t){
        if(t){
            vector = new short[100];
            for (int i=0;i<100;i++){
                vector[i] = Short.MIN_VALUE;
            }
        }
        else{
            vector = new short[100];
            for (int i=0;i<100;i++){
                vector[i] = Short.MAX_VALUE;
            }
        }
    }
}
