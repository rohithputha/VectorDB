package LSHFIndex;

import global.PageId;
import global.Vector100Dtype;

import java.util.Arrays;

public class LSHLayer {
    private int[] hashFunctions;
    private int layerStartPage;
    private int layerId;
    private String indexName;
    public LSHLayer(int[] hashFunctions, int layerStartPage, int layerId, String indexName) {
        this.hashFunctions = Arrays.copyOf(hashFunctions, hashFunctions.length);
        this.layerStartPage = layerStartPage;
        this.layerId = layerId;
        this.indexName = indexName;
    }

    public int getLayerId(){
        return layerId;
    }

    public int getLayerStartPage(){
        return layerStartPage;
    }


    public int[] getCompoundHash(Vector100Dtype v, LSHashFunctionsMap lsHashFunctionsMap) {
        int[] compoundHash = new int[hashFunctions.length];
        for (int i = 0; i < hashFunctions.length; i++) {
            compoundHash[i] = lsHashFunctionsMap.getHashFunction(hashFunctions[i], indexName).getHashValue(v);
        }
        return compoundHash;
    }

    public int[] getCompoundHash(Vector100Dtype v) {
        int[] compoundHash = new int[hashFunctions.length];
        for (int i = 0; i < hashFunctions.length; i++) {
            compoundHash[i] = LSHashFunctionsMap.getInstance(this.indexName).getHashFunction(hashFunctions[i], indexName).getHashValue(v);
        }
        return compoundHash;
    }
}
