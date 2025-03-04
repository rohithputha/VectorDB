package LSHFIndex;

import global.Vector100Dtype;

import java.util.Arrays;

public class LSHLayer {
    private int[] hashFunctions;
    private int layerStartPage;
    private int layerId;

    public LSHLayer(int[] hashFunctions, int layerStartPage, int layerId) {
        this.hashFunctions = Arrays.copyOf(hashFunctions, hashFunctions.length);
        this.layerStartPage = layerStartPage;
        this.layerId = layerId;
    }

    public int getLayerId(){
        return layerId;
    }

    public int getLayerStartPage(){
        return layerStartPage;
    }
    public int[] getCompoundHash(Vector100Dtype v) {
        int[] compoundHash = new int[hashFunctions.length];
        for (int i = 0; i < hashFunctions.length; i++) {
//            compoundHash[i] = hashFunctions[i].getHashValue(v);
        }
        return compoundHash;
    }
}