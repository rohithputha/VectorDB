package LSHFIndex;

import btree.ConstructPageException;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;

public class LSHFInnerPage extends HFPage {
    private int layerId;
    private PageId pageId;
    public static int PageType=5;


    public LSHFInnerPage(PageId pageId) throws ConstructPageException {
        super();
        super.curPage = pageId;
        this.pageId = pageId;
        try{
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
        }

    }

    public LSHFInnerPage(int layerId)throws ConstructPageException {
        new LSHFInnerPage(layerId,0);
    }

    public LSHFInnerPage(int layerId, int hashInConsideration)throws ConstructPageException {
        super();
        try{
            Page newPage = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
            if (newPageId == null){
                throw new ConstructPageException(null, "new page failed. failed to construct LSHHeaderPage");
            }
            this.init(newPageId, newPage);

            // Metadata tuple: [type, hashInConsideration, layerId]
            Tuple t = new Tuple();
            t.setHdr((short)3, new AttrType[]{
                new AttrType(AttrType.attrInteger), // Field 1: type (1 for inner)
                new AttrType(AttrType.attrInteger), // Field 2: hashInConsideration
                new AttrType(AttrType.attrInteger)  // Field 3: layerId
            }, null);
            t.setIntFld(1, 5); // Type: 1 for inner
            t.setIntFld(2, hashInConsideration);
            t.setIntFld(3, layerId);
            RID rid = this.insertRecord(t.getTupleByteArray());
            this.layerId =  layerId;
        } catch (Exception e){
            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
        }
    }

    // Method to get page type from metadata tuple
    
    public short getPageType() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage, 0));
        t.setHdr((short)3, new AttrType[]{
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
        }, null);
        return (short) t.getIntFld(1); // Field 1 is type
    }

    

    public PageId getPageId(){
        return pageId;
    }
    public int getLayerId(){
        return layerId;
    }
    public int getHashFunctionInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
       Tuple t = this.getRecord(new RID(this.curPage,0));
       t.setHdr((short)3, new AttrType[]{
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrInteger)
    }, null);
    return t.getIntFld(2);
    }
    private int getLayerInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage, 0));
        t.setHdr((short)3, new AttrType[]{
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
        }, null);
        return t.getIntFld(3); // Field 3 is layerId
    }

    public int getBucketByKey(Vector100Dtype v) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException {
        int[] compoundHash = LSHLayerMap.getInstance().getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v);
        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];

        for (short i = 1;i<this.getSlotCnt();i++){
            Tuple t = this.getRecord(new RID(this.curPage,i));
            t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
            if (t.getIntFld(1) == hashInConsideration){
                return t.getIntFld(2);
            }
        }
        return -1;
    }

    public RID insertBucketByKey(Vector100Dtype v, int pid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException {
        int bucket = getBucketByKey(v);
        if (bucket != -1){
            return null;
        }

        Tuple t = new Tuple();
        t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        int[] compoundHash = LSHLayerMap.getInstance().getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v);
        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];
        t.setIntFld(1, hashInConsideration);
        t.setIntFld(2, pid);
        return this.insertRecord(t.getTupleByteArray());
    }

    public int getFirstBucketPageId() throws Exception {
        if (getSlotCnt() < 2) return -1; // Slot 0 is metadata, no buckets yet
        RID firstBucketRid = new RID(getCurPage(), 1); // First bucket at slot 1
        Tuple t = getRecord(firstBucketRid);
        if (t == null) return -1;
        t.setHdr((short)2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(2); // Field 2 is the page ID
    }

    
}
/*
    (hash function in consideration -> , layer id)
 */

/*
    (key which is used as a comparison -> out of h hash functions, which one is used to compare as part of the prefix tree)
    [(various buckets of hash function h[k],pageIds of the ptr page)]
 */
