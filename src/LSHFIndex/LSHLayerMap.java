package LSHFIndex;

import java.util.*;

public class LSHLayerMap implements Iterable<LSHLayer> {
    private static LSHLayerMap instance;

    private Map<Integer, LSHLayer> lshLayerMap;
    private int totalNumLayers;

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

    public static void addLayer(LSHLayer lshLayer) {
        if (instance != null) {
            return;
        }
        if (layerList == null) {
            layerList = new ArrayList<>();
        }
        layerList.add(lshLayer);
    }

    public static LSHLayerMap getInstance() {
        if (instance != null) {
            return instance;
        }
        instance = new LSHLayerMap();
        for (LSHLayer lshLayer : layerList) {
            instance.addLayerToMap(lshLayer);
        }
        return instance;
    }


    @Override
    public Iterator<LSHLayer> iterator() {
        return new LSHLayerIterator(this.lshLayerMap);
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