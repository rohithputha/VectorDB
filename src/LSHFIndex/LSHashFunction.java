package LSHFIndex;

import global.Vector100Dtype;

import java.util.Arrays;

public class LSHashFunction {
    private int[] vectA;
    private int constB;
    private static final int maxB = 100; // should be configurable.
    public LSHashFunction(int[] vectA, int constB) {
        this.vectA = Arrays.copyOf(vectA, vectA.length);
        this.constB = constB;
    }

    public int getHashValue(Vector100Dtype v){
        int hash = 0;
        for(int i = 0; i < 100;i++){
            hash = hash + (v.get(i)*vectA[i]);
        }
        hash = hash + constB;
        hash = hash % maxB;
        return hash;
    }
}