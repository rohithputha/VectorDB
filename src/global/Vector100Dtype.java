package global;

public class Vector100Dtype{
    private static final int DIMENSIONS = 100;
    private short[] vector;

    //normal constructor
    public Vector100Dtype(){
        vector = new short[DIMENSIONS];
    }

    //initiating with some values
    public Vector100Dtype(short[] values){
        if(values.length != DIMENSIONS){
            throw new IllegalArgumentException("Length less than 100");
        }
        vector = new short[DIMENSIONS];
        for(int i=0; i<DIMENSIONS; i++){
            if(values[i]<-10000 || values[i]>10000){
                throw new IllegalArgumentException("Value out of bound");
            }
            vector[i]=values[i];
        }
    }

    public int distanceTo(Vector100Dtype other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot compute distance to null vector");
        }
        long sum = 0; // Use long to avoid overflow during summation
        for (int i = 0; i < DIMENSIONS; i++) {
            int diff = this.vector[i] - other.vector[i];
            sum += diff * diff;
        }
        return (int) Math.sqrt(sum); // Cast to int as per project spec
    }

    public short[] getVector() {
        return vector.clone();
    }

}