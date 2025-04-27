//package LSHFIndex;
//
//import btree.ConstructPageException;
//import bufmgr.HashEntryNotFoundException;
//import bufmgr.InvalidFrameNumberException;
//import bufmgr.PageUnpinnedException;
//import bufmgr.ReplacerException;
//import diskmgr.Page;
//import global.*;
//import heap.*;
//
//import java.io.IOException;
//import java.util.*;
//
//public class LSHFInnerPage extends LSHBasePage implements Iterable<LSHDto> {
//    private int layerId;
//    private PageId pageId;
//    public static final int pageType = 5;
//    private final String indexName;
//    private PageId baseInnerPageId;
//
//    private LSHFInnerPage(PageId pageId, String indexName, PageId baseInnerPageId) throws ConstructPageException {
//        this(pageId, indexName);
//        this.baseInnerPageId = baseInnerPageId;
//    }
//
//    private LSHFInnerPage(int layerId, int hashInConsideration, String indexName, PageId baseInnerPageId) throws ConstructPageException {
//        this(layerId, hashInConsideration, indexName);
//        this.baseInnerPageId = baseInnerPageId;
//
//    }
//
//    public LSHFInnerPage(PageId pageId, String indexName) throws ConstructPageException {
//        super(indexName);
//        this.indexName = indexName;
//        this.pageId = pageId;
//        try {
//            super.setCurPage(pageId);
//            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
////            this.init(pageId,this);
//        } catch (Exception e) {
//            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
//        }
//
//    }
//
//    public LSHFInnerPage(int layerId, String indexName) throws ConstructPageException {
//        this(layerId, 0, indexName);
//    }
//
//    public LSHFInnerPage(LSHBasePage basePage, String indexName) throws ConstructPageException, IOException {
//        super(indexName);
//        this.indexName = indexName;
//        this.pageId = basePage.getCurPage();
//        try {
//            this.setCurPage(basePage.getCurPage());
//            this.setpage(basePage.getpage());
//        } catch (Exception e) {
//            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
//        }
//
//    }
//
//    public LSHFInnerPage(int layerId, int hashInConsideration, String indexName) throws ConstructPageException {
//        super(indexName);
//        this.indexName = indexName;
//        try {
//            Page newPage = new Page();
//            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
//            if (newPageId == null) {
//                throw new ConstructPageException(null, "new page failed. failed to construct LSHHeaderPage");
//            }
//            this.init(newPageId, newPage);
//            this.pageId = newPageId;
//
//            Tuple pageTuple = new Tuple();
//            pageTuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
//            pageTuple.setIntFld(1, LSHFInnerPage.pageType);
//            this.insertRecord(pageTuple.getTupleByteArray());
//
//            Tuple t = new Tuple();
//            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//            t.setIntFld(1, hashInConsideration);
//            t.setIntFld(2, layerId);
//            this.insertRecord(t.getTupleByteArray());
//
//            this.layerId = layerId;
//        } catch (Exception e) {
//            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
//        }
//    }
//
//    public PageId getPageId() {
//        return pageId;
//    }
//
//    public int getLayerId() {
//        return layerId;
//    }
//
//    public int getHashFunctionInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
//        Tuple t = this.getRecord(new RID(this.curPage, 1));
//        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//        return t.getIntFld(1);
//    }
//
//    private int getLayerInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
//        Tuple t = this.getRecord(new RID(this.curPage, 1));
//        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//        return t.getIntFld(2);
//    }
//
//    public int getHashInConsideration(Vector100Dtype v) throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTypeException, InvalidTupleSizeException {
//        int[] compoundHash = LSHLayerMap.getInstance(this.indexName).getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v, LSHashFunctionsMap.getInstance(indexName));
//        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];
//        return hashInConsideration;
//    }
//
//    private LSHFInnerPage getNextInnerPage(boolean thisDirty) throws IOException, ConstructPageException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//        if (this.getNextPage().pid!=INVALID_PAGE){
//            LSHFInnerPage nextInnerPage = new LSHFInnerPage(this.getNextPage(), this.indexName);
//            if (this.baseInnerPageId!= null && this.baseInnerPageId.pid != this.pageId.pid){
//                SystemDefs.JavabaseBM.unpinPage(this.pageId, thisDirty);
//            }
//            return nextInnerPage;
//        }
//        return null;
//
//    }
//    private void unpinListInnerPage(PageId pageId, boolean thisDirty) throws HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//        if (this.baseInnerPageId!= null && this.baseInnerPageId.pid == this.pageId.pid){
//            return;
//        }
//        SystemDefs.JavabaseBM.unpinPage(pageId,thisDirty);
//    }
//
//    public int getBucketByKey(Vector100Dtype v) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, ConstructPageException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//        int hashInConsideration = this.getHashInConsideration(v);
//        for (int i = 2; i < this.getSlotCnt(); i++) {
//            Tuple t = this.getRecord(new RID(this.getCurPage(), i));
//            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//            if (t.getIntFld(1) == hashInConsideration) {
//                unpinListInnerPage(this.getCurPage(), false);
//                return t.getIntFld(2);
//            }
//        }
//
//        LSHFInnerPage nextInnerPage = getNextInnerPage(false);
//        if (nextInnerPage!=null){
//            return nextInnerPage.getBucketByKey(v);
//        }
//        unpinListInnerPage(this.getCurPage(), false);
//        return -1;
//    }
//
//    public boolean deleteBucketByPageId(int pageId) throws InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, HashEntryNotFoundException, ConstructPageException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//        for (short i = 2; i < this.getSlotCnt(); i++) {
//            Tuple t = this.getRecord(new RID(this.getCurPage(), i));
//            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//            if (t.getIntFld(2) == pageId) {
//                this.deleteRecord(new RID(this.getCurPage(), i));
//                unpinListInnerPage(this.getCurPage(), true);
//                return true;
//            }
//        }
//        LSHFInnerPage nextInnerPage = getNextInnerPage(false);
//        if (nextInnerPage!=null){
//            return nextInnerPage.deleteBucketByPageId(pageId);
//        }
//        unpinListInnerPage(this.getCurPage(), false);
//        return false;
//    }
//    public RID insertBucketByKey(Vector100Dtype v, int pid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, ConstructPageException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//        if (this.available_space()<20){
//            LSHFInnerPage nextInnerPage = this.getNextInnerPage(false);
//            if (nextInnerPage!=null){
//                unpinListInnerPage(this.getCurPage(), false);
//                return nextInnerPage.insertBucketByKey(v, pid);
//            }
//            else{
//                PageId bPid = this.baseInnerPageId!= null ? baseInnerPageId: this.pageId;
//                LSHFInnerPage innerPage = new LSHFInnerPage(this.layerId,this.getHashFunctionInConsideration(), this.indexName,bPid);
//                this.setNextPage(innerPage.getCurPage());
//                unpinListInnerPage(this.getCurPage(), true);
//                return innerPage.insertBucketByKey(v, pid);
//            }
//        }
//        Tuple t = new Tuple();
//        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//        int hashInConsideration = this.getHashInConsideration(v);
//        t.setIntFld(1, hashInConsideration);
//        t.setIntFld(2, pid);
//        RID rid =  this.insertRecord(t.getTupleByteArray());
//        unpinListInnerPage(this.getCurPage(), true);
//        return rid;
//    }
//
//    @Override
//    public LSHInnerPageIterator iterator() {
//        try {
//            return new LSHInnerPageIterator(this);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    public Iterator<List<LSHDto>> expansionIterator(int hash) throws IOException {
//
//        List<LSHDto> lshDtos = new ArrayList<>();
//        Iterator<LSHDto> iterator = iterator();
//        while (iterator.hasNext()) {
//            lshDtos.add(iterator.next());
//        }
//        Collections.sort(lshDtos);
//        int startIndex = 0;
//        for (short i = 0; i < lshDtos.size(); i++) {
//            if(hash>=lshDtos.get(i).getHash()){
//                startIndex = i;
//            }
//        }
//        if (startIndex != lshDtos.size()-1){
//            startIndex = startIndex+1;
//        }
//        return new LshInnerPageExpansionIterator(startIndex, lshDtos);
//    }
//
//    public class LSHInnerPageIterator implements Iterator<LSHDto> {
//
//        private int lm;
//        private int rm;
//        private int i;
//        private LSHFInnerPage innerPage;
//        private final LSHFInnerPage baseInnerPage;
//
//        private LSHInnerPageIterator(LSHFInnerPage innerPage) throws IOException {
//            this.lm = 2;
//            this.rm = innerPage.getSlotCnt() - 1;
//            this.i = lm;
//            this.innerPage = innerPage;
//            this.baseInnerPage = innerPage;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if(this.i > this.rm){
//                try {
//                    if(this.innerPage.getNextPage().pid != INVALID_PAGE || this.innerPage.getNextPage().pid != 0){
//                        this.innerPage = this.innerPage.getNextInnerPage(false);
//                        this.lm = 2;
//                        this.rm = innerPage.getSlotCnt()-1;
//                        this.i = lm;
//                    }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                } catch (HashEntryNotFoundException e) {
//                    throw new RuntimeException(e);
//                } catch (ConstructPageException e) {
//                    throw new RuntimeException(e);
//                } catch (InvalidFrameNumberException e) {
//                    throw new RuntimeException(e);
//                } catch (PageUnpinnedException e) {
//                    throw new RuntimeException(e);
//                } catch (ReplacerException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            return this.i <= this.rm;
//        }
//
//        @Override
//        public LSHDto next() {
//            Tuple t = null;
//            try {
//                t = innerPage.getRecord(new RID(innerPage.getCurPage(), i));
//                this.i++;
//                t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//                return new LSHDto(t.getIntFld(1), t.getIntFld(2));
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            } catch (InvalidSlotNumberException e) {
//                throw new RuntimeException(e);
//            } catch (InvalidTupleSizeException e) {
//                throw new RuntimeException(e);
//            } catch (InvalidTypeException e) {
//                throw new RuntimeException(e);
//            } catch (FieldNumberOutOfBoundException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//    }
//
//    private class LshInnerPageExpansionIterator implements Iterator<List<LSHDto>> {
//
//        private int l;
//        private int r;
//        private final int startIndex;
//        private final int lm;
//        private final int rm;
//        private List<LSHDto> dtos;
//
//        private LshInnerPageExpansionIterator(int stIndex, List<LSHDto> dtos) throws IOException {
//            startIndex = stIndex;
//            l = stIndex - 1;
//            r = stIndex + 1;
//            lm = 0;
//            rm = dtos.size() - 1;
//            this.dtos = dtos;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (l < lm && r > rm) {
//                return false;
//            }
//            return true;
//        }
//
//        @Override
//        public List<LSHDto> next() {
//            List<LSHDto> lshDtos = new ArrayList<LSHDto>();
//            if (r <= rm) {
//                lshDtos.add(dtos.get(r));
//                r++;
//            }
//            if (l >= lm) {
//                lshDtos.add(dtos.get(l));
//                l--;
//            }
//
//            return lshDtos;
//        }
//    }
//}
//
///*
//   (pageType) (hash function in consideration -> , layer id) (rest of tuples)
// */
//
///*
//    (key which is used as a comparison -> out of h hash functions, which one is used to compare as part of the prefix tree)
//    [(various buckets of hash function h[k],pageIds of the ptr page)]
// */



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
    private final String indexName;

    public LSHFInnerPage(PageId pageId, String indexName) throws ConstructPageException {
        super(indexName);
        this.indexName = indexName;
        this.pageId = pageId;
        try {
            super.setCurPage(pageId);
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
//            this.init(pageId,this);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
        }

    }

    public LSHFInnerPage(int layerId, String indexName) throws ConstructPageException {
        this(layerId, 0, indexName);
    }

    public LSHFInnerPage(LSHBasePage basePage, String indexName) throws ConstructPageException, IOException {
        super(indexName);
        this.indexName = indexName;
        this.pageId = basePage.getCurPage();
        try {
            this.setCurPage(basePage.getCurPage());
            this.setpage(basePage.getpage());
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHInnerPage");
        }

    }

    public LSHFInnerPage(int layerId, int hashInConsideration, String indexName) throws ConstructPageException {
        super(indexName);
        this.indexName = indexName;
        try {
            Page newPage = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
            if (newPageId == null) {
                throw new ConstructPageException(null, "new page failed. failed to construct LSHHeaderPage");
            }
            this.init(newPageId, newPage);
            this.pageId = newPageId;

            Tuple pageTuple = new Tuple();
            pageTuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            pageTuple.setIntFld(1, LSHFInnerPage.pageType);
            this.insertRecord(pageTuple.getTupleByteArray());

            Tuple t = new Tuple();
            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            t.setIntFld(1, hashInConsideration);
            t.setIntFld(2, layerId);
            this.insertRecord(t.getTupleByteArray());

            this.layerId = layerId;
        } catch (Exception e) {
            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
        }
    }

    public PageId getPageId() {
        return pageId;
    }

    public int getLayerId() {
        return layerId;
    }

    public int getHashFunctionInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage, 1));
        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(1);
    }

    private int getLayerInConsideration() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.curPage, 1));
        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(2);
    }

    public int getHashInConsideration(Vector100Dtype v) throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTypeException, InvalidTupleSizeException {
        int[] compoundHash = LSHLayerMap.getInstance(this.indexName).getLayerByLayerId(this.getLayerInConsideration()).getCompoundHash(v, LSHashFunctionsMap.getInstance(indexName));
        int hashInConsideration = compoundHash[getHashFunctionInConsideration()];
        return hashInConsideration;
    }

    public int getBucketByKey(Vector100Dtype v) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException {
        int hashInConsideration = this.getHashInConsideration(v);
        for (int i = 2; i < this.getSlotCnt(); i++) {
            Tuple t = this.getRecord(new RID(this.getCurPage(), i));
            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            if (t.getIntFld(1) == hashInConsideration) {
                return t.getIntFld(2);
            }
        }
        return -1;
    }

    public boolean deleteBucketByPageId(int pageId) throws InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException {
        for (short i = 2; i < this.getSlotCnt(); i++) {
            Tuple t = this.getRecord(new RID(this.getCurPage(), i));
            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            if (t.getIntFld(2) == pageId) {
                this.deleteRecord(new RID(this.getCurPage(), i));
                return true;
            }
        }
        this.compact_slot_dir();
        return false;
    }
    public RID insertBucketByKey(Vector100Dtype v, int pid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = new Tuple();
        t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        int hashInConsideration = this.getHashInConsideration(v);
        t.setIntFld(1, hashInConsideration);
        t.setIntFld(2, pid);
        if(this.available_space()<20){
            System.out.println("low available space");
        }
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


    public Iterator<List<LSHDto>> expansionIterator(int hash) throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, FieldNumberOutOfBoundException {

        List<LSHDto> lshDtos = new ArrayList<>();
        for (short i = 2; i < this.getSlotCnt(); i++) {
            Tuple t = this.getRecord(new RID(this.curPage, i));
            t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
//            if (t.getIntFld(2) == startPage) {
//                return new LshInnerPageExpansionIterator(i, this);
//            }

            lshDtos.add(new LSHDto(t.getIntFld(1),t.getIntFld(2)));
        }
        Collections.sort(lshDtos);
        int startIndex = 0;
        for (short i = 0; i < lshDtos.size(); i++) {
            if(hash>=lshDtos.get(i).getHash()){
                startIndex = i;
            }
        }
        if (startIndex != lshDtos.size()-1){
            startIndex = startIndex+1;
        }
        return new LshInnerPageExpansionIterator(startIndex, lshDtos);
    }

    public class LSHInnerPageIterator implements Iterator<LSHDto> {

        private final int lm;
        private final int rm;
        private int i;
        private LSHFInnerPage innerPage;

        private LSHInnerPageIterator(LSHFInnerPage innerPage) throws IOException {
            this.lm = 2;
            this.rm = innerPage.getSlotCnt() - 1;
            this.i = lm;
            this.innerPage = innerPage;
        }

        @Override
        public boolean hasNext() {
            return this.i <= this.rm;
        }

        @Override
        public LSHDto next() {
            Tuple t = null;
            try {
                t = innerPage.getRecord(new RID(innerPage.getCurPage(), i));
                this.i++;
                t.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
                return new LSHDto(t.getIntFld(1), t.getIntFld(2));
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

    }

    private class LshInnerPageExpansionIterator implements Iterator<List<LSHDto>> {

        private int l;
        private int r;
        private final int startIndex;
        private final int lm;
        private final int rm;
        private List<LSHDto> dtos;

        private LshInnerPageExpansionIterator(int stIndex, List<LSHDto> dtos) throws IOException {
            startIndex = stIndex;
            l = stIndex - 1;
            r = stIndex + 1;
            lm = 0;
            rm = dtos.size() - 1;
            this.dtos = dtos;
        }

        @Override
        public boolean hasNext() {
            if (l < lm && r > rm) {
                return false;
            }
            return true;
        }

        @Override
        public List<LSHDto> next() {
            List<LSHDto> lshDtos = new ArrayList<LSHDto>();
            if (r <= rm) {
                lshDtos.add(dtos.get(r));
                r++;
            }
            if (l >= lm) {
                lshDtos.add(dtos.get(l));
                l--;
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

