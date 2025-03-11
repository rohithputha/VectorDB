package LSHFIndex;

import btree.ConstructPageException;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.*;

public class LSHFInnerPage extends LSHBasePage implements Iterable<LSHDto> {
    private int layerId;
    private PageId pageId;
    public static final int pageType = 5;

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

    public LSHFInnerPage(LSHBasePage basePage) throws ConstructPageException, IOException {
        super();
        this.curPage = basePage.getCurPage();
        this.pageId = basePage.getCurPage();
        try{
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
            SystemDefs.JavabaseBM.unpinPage(pageId, false);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
        }

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
            this.pageId = newPageId;

            Tuple pageTuple = new Tuple();
            pageTuple.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            pageTuple.setIntFld(1, LSHFInnerPage.pageType);
            this.insertRecord(pageTuple.getTupleByteArray());

            Tuple t = new Tuple();
            t.setHdr((short)2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            t.setIntFld(1, hashInConsideration);
            t.setIntFld(2, layerId);
            this.insertRecord(t.getTupleByteArray());

            this.layerId =  layerId;
        } catch (Exception e){
            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
        }
    }

    public PageId getPageId(){
        return pageId;
    }
    public int getLayerId(){
        return layerId;
    }
    public int getHashFunctionInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage,1));
        t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(1);
    }
    private int getLayerInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage,1));
        t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(2);
    }

    public int getBucketByKey(Vector100Dtype v) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException {
        int[] compoundHash = LSHLayerMap.getInstance().getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v, LSHashFunctionsMap.getInstance());
        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];

        for (short i = 2;i<this.getSlotCnt();i++){
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
        int[] compoundHash = LSHLayerMap.getInstance().getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v, LSHashFunctionsMap.getInstance());
        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];
        t.setIntFld(1, hashInConsideration);
        t.setIntFld(2, pid);
        return this.insertRecord(t.getTupleByteArray());
    }

    @Override
    public LSHInnerPageIterator iterator() {
        try {
            return new LSHInnerPageIterator(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public Iterator<List<LSHDto>> expansionIterator(int startPage) throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, FieldNumberOutOfBoundException {

        for (short i = 2;i<this.getSlotCnt();i++){
            Tuple t = this.getRecord(new RID(this.curPage,i));
            t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
            if (t.getIntFld(2) == startPage){
                return new LshInnerPageExpansionIterator(i, this);
            }
        }
        return null;
    }

    public class LSHInnerPageIterator implements Iterator<LSHDto> {

        private final int lm;
        private final int rm;
        private int i;
        private LSHFInnerPage innerPage;
        private Set<Integer> exceptions;
        private LSHInnerPageIterator(LSHFInnerPage innerPage) throws IOException {
            this.lm = 2;
            this.rm = innerPage.getSlotCnt() - 1;
            this.i = lm;
            this.innerPage = innerPage;
            this.exceptions = new HashSet<Integer>();
        }
        @Override
        public boolean hasNext() {
            return this.i <= this.rm;
        }

        @Override
        public LSHDto next() {
            Tuple t= null;
            try {
                t = innerPage.getRecord(new RID(innerPage.getCurPage(),i));
                this.i++;
                t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
                if (exceptions.contains(t.getIntFld(2))){
                    if(hasNext()){
                        return next();
                    }
                    else return new LSHDto(-1, -1);
                }
                return new LSHDto(t.getIntFld(1),t.getIntFld(2));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InvalidSlotNumberException e) {
                throw new RuntimeException(e);
            } catch (InvalidTupleSizeException e) {
                throw new RuntimeException(e);
            } catch (InvalidTypeException e) {
                throw new RuntimeException(e);
            } catch (FieldNumberOutOfBoundException e) {
                throw new RuntimeException(e);
            }
        }

        public void setExceptions(int pid){
            exceptions.add(pid);
        }
    }

    private class LshInnerPageExpansionIterator implements Iterator<List<LSHDto>>{

        private int l;
        private int r;
        private final int startIndex;
        private final int lm;
        private final int rm;
        private final LSHFInnerPage innerPage;

        private LshInnerPageExpansionIterator(int stIndex, LSHFInnerPage innerPage) throws IOException {
            startIndex = stIndex;
            l = stIndex-1;
            r = stIndex+1;
            lm = 2;
            rm = innerPage.getSlotCnt()-1;
            this.innerPage = innerPage;
        }
        @Override
        public boolean hasNext() {
            if(l<lm && r>rm){
                return false;
            }
            return true;
        }

        @Override
        public List<LSHDto> next() {
            List<LSHDto> lshDtos = new ArrayList<LSHDto>();
            if (l>=lm){
                try {
                    Tuple t= innerPage.getRecord(new RID(innerPage.curPage,l));
                    t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
                    lshDtos.add(new LSHDto(t.getIntFld(1),t.getIntFld(2)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InvalidSlotNumberException e) {
                    throw new RuntimeException(e);
                } catch (InvalidTupleSizeException e) {
                    throw new RuntimeException(e);
                } catch (FieldNumberOutOfBoundException e) {
                    throw new RuntimeException(e);
                } catch (InvalidTypeException e) {
                    throw new RuntimeException(e);
                }
            }
            if (r<=rm){
                try {
                    Tuple t= innerPage.getRecord(new RID(innerPage.curPage,r));
                    t.setHdr((short)2,new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
                    lshDtos.add(new LSHDto(t.getIntFld(1),t.getIntFld(2)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InvalidSlotNumberException e) {
                    throw new RuntimeException(e);
                } catch (InvalidTupleSizeException e) {
                    throw new RuntimeException(e);
                } catch (FieldNumberOutOfBoundException e) {
                    throw new RuntimeException(e);
                } catch (InvalidTypeException e) {
                    throw new RuntimeException(e);
                }
            }

            return lshDtos;
        }
    }
}

/*
   (pageType) (hash function in consideration -> , layer id) (rest of tuples)
 */

/*
    (key which is used as a comparison -> out of h hash functions, which one is used to compare as part of the prefix tree)
    [(various buckets of hash function h[k],pageIds of the ptr page)]
 */
