package LSHFIndex;

import global.Vector100Dtype;

public class LSHHashDto extends LSHDto implements Comparable<LSHHashDto> {

    private int[] compoundHash;
    private int layerId;
    public LSHHashDto(Vector100Dtype v, int pid, int sid, int layerId, String indexName) {
        super(v, pid, sid);
        this.layerId = layerId;
        compoundHash = LSHLayerMap.getInstance(indexName).getLayerByLayerId(layerId).getCompoundHash(v, LSHashFunctionsMap.getInstance(indexName));
    }
    public LSHHashDto(LSHDto lshDto, int layerId,  String indexName) {
        this(lshDto.getV(), lshDto.getPid(), lshDto.getSid(), layerId, indexName);
    }

    @Override
    public int compareTo(LSHHashDto o) {
        for (int i = 0; i < compoundHash.length; i++) {
            int compareVal = Integer.compare(compoundHash[i], o.compoundHash[i]);
            if (compareVal != 0) {
                return compareVal;
            }
        }
        return 0;
    }


}
