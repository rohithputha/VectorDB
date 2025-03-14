package LSHFIndex;

import java.util.*;

public class LSHLayerMap implements Iterable<LSHLayer> {
//    private static LSHLayerMap instance;

    private Map<Integer, LSHLayer> lshLayerMap;
    private int totalNumLayers;
    private static Map<String, LSHLayerMap> indexLayerMap = new HashMap<String, LSHLayerMap>();
    private static List<LSHLayer> layerList;


    private LSHLayerMap() {
        lshLayerMap = new HashMap<>();
        this.totalNumLayers = 0;
    }
    private void addLayerToMap(LSHLayer lshLayer){
        lshLayerMap.put(lshLayer.getLayerId(),lshLayer);
        totalNumLayers++;
    }

    public LSHLayer getLayerByLayerId(int layerId){
        return this.lshLayerMap.get(layerId);
    }

    public static void addLayer(LSHLayer lshLayer, String indexName) {
        if (indexLayerMap.containsKey(indexName)) {
            return;
        }
        if (layerList == null) {
            layerList = new ArrayList<>();
        }
        layerList.add(lshLayer);
    }

    public static LSHLayerMap getInstance(String indexName) {
        if (indexLayerMap.containsKey(indexName)) {
            return indexLayerMap.get(indexName);
        }
        LSHLayerMap instance = new LSHLayerMap();
        for (LSHLayer lshLayer : layerList) {
            instance.addLayerToMap(lshLayer);
        }
        indexLayerMap.put(indexName, instance);
        return instance;
    }

    public Map<Integer, LSHLayer> getLshLayerMap() {
        return lshLayerMap;

    }


    @Override
    public Iterator<LSHLayer> iterator() {
        return new LSHLayerIterator(this.lshLayerMap);
    }

    public Iterator<LSHLayer> layerIteratorByIndexName(String indexName) {
        return new LSHLayerIterator(indexLayerMap.get(indexName).getLshLayerMap());
    }

     class LSHLayerIterator implements Iterator<LSHLayer> {

        int index = 0;
        List<LSHLayer> layerList;
        public LSHLayerIterator(Map<Integer, LSHLayer> layerMap) {
            layerList = new ArrayList<>(layerMap.values());
        }

        @Override
        public boolean hasNext() {
            if (index < layerList.size()) return true;
            return false;
        }

        @Override
        public LSHLayer next() {
            return layerList.get(index++);
        }
    }
}
