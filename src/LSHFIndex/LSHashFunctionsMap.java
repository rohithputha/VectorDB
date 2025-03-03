package LSHFIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LSHashFunctionsMap {
    private static LSHashFunctionsMap instance;

    private Map<Integer, LSHashFunction> hashFunctionsMap;

    private static List<LSHashFunction> listOfHashFunctions;



    private int totalNumHashFunctions;
    private LSHashFunctionsMap() {
        this.hashFunctionsMap = new HashMap<>();
        this.totalNumHashFunctions = 0;
    }
    private void addHashFunctionToMap(LSHashFunction hashFunction) {
        this.hashFunctionsMap.put(totalNumHashFunctions, hashFunction);
        totalNumHashFunctions++;
    }

    public LSHashFunction getHashFunction(int numHashFunctions) {
        return this.hashFunctionsMap.get(numHashFunctions);
    }

    public int getTotalNumHashFunctions() {
        return this.totalNumHashFunctions;
    }


   public static void addHashFunction(LSHashFunction hashFunction) {
        if (instance != null) {
            return;
        }
        if (listOfHashFunctions == null) {
            listOfHashFunctions = new ArrayList<>();
        }
        listOfHashFunctions.add(hashFunction);
   }

    public static LSHashFunctionsMap getInstance() {

        if (instance != null) {
            return instance;
        }
        LSHashFunctionsMap instance = new LSHashFunctionsMap();
        for (LSHashFunction hashFunction : listOfHashFunctions) {
            instance.addHashFunctionToMap(hashFunction);
        }
        listOfHashFunctions = new ArrayList<>();
        return instance;
    }
}
