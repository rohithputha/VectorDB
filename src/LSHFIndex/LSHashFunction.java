package LSHFIndex;

import global.Vector100Dtype;

import java.util.Arrays;

public class LSHashFunction {
    private int[] vectA;
    private int constB;
    private static final int maxB = 10000; // should be configurable.
    public LSHashFunction(int[] vectA, int constB) {
        this.vectA = Arrays.copyOf(vectA, vectA.length);
        this.constB = constB;
    }

    public int getHashValue(Vector100Dtype v){
        long hash = 0L;
        for(int i = 0; i < 100;i++){
            hash = hash + (((long) v.get(i) * vectA[i]))/1000;
        }
        hash = hash + constB;
        hash = hash /1000;
        return (int)hash;
    }
}
