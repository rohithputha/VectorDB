package btree;

public class RealKey extends KeyClass {
    private Float key;

    public RealKey(Float value) {
        key = value;
    }

    public Float getKey() {
        return key;
    }

    public void setKey(Float value) {
        key = value;
    }

    public String toString() {
        return Float.toString(key);
    }
}
