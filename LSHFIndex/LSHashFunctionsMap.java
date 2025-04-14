package LSHFIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LSHashFunctionsMap {


    private Map<Integer, LSHashFunction> hashFunctionsMap;

    private static List<LSHashFunction> listOfHashFunctions;

    private static Map<String, LSHashFunctionsMap> indexHashFunctionMap = new HashMap<>();


    private int totalNumHashFunctions;
    private LSHashFunctionsMap() {
        this.hashFunctionsMap = new HashMap<>();
        this.totalNumHashFunctions = 0;
    }
    private void addHashFunctionToMap(LSHashFunction hashFunction) {
        this.hashFunctionsMap.put(totalNumHashFunctions, hashFunction);
        totalNumHashFunctions++;
    }

    public LSHashFunction getHashFunction(int numHashFunctions, String indexName) {
        return indexHashFunctionMap.get(indexName).hashFunctionsMap.get(numHashFunctions);
    }

    public int getTotalNumHashFunctions() {
        return this.totalNumHashFunctions;
    }


   public static void addHashFunction(LSHashFunction hashFunction, String indexName) {
        if (indexHashFunctionMap.containsKey(indexName)) {
            return;
        }
        if (listOfHashFunctions == null) {
            listOfHashFunctions = new ArrayList<>();
        }
        listOfHashFunctions.add(hashFunction);
   }

    public static LSHashFunctionsMap getInstance(String indexName) {

        if (indexHashFunctionMap.containsKey(indexName)) {
            return indexHashFunctionMap.get(indexName);
        }
        LSHashFunctionsMap instance = new LSHashFunctionsMap();
        for (LSHashFunction hashFunction : listOfHashFunctions) {
            instance.addHashFunctionToMap(hashFunction);
        }
        listOfHashFunctions = new ArrayList<>();
        indexHashFunctionMap.put(indexName, instance);
        return instance;
    }
}
