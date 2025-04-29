package LSHFIndex;

import btree.ConstructPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.*;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LSHFLeafPage extends LSHBasePage implements Iterable<LSHDto> {
    private PageId pageId;
    public static final int pageType = 4;

    public LSHFLeafPage(String indexName) throws ConstructPageException {
        super(indexName);
        try{
            Page newPage = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
            if (newPageId == null){
                throw new ConstructPageException(null, "new page failed. failed to construct LSHLeafPage");
            }
            this.init(newPageId, newPage); // this is where it gets associated with the current page
            this.pageId = newPageId;
//            this.setNextPage(new PageId(INVALID_PAGE));
            Tuple t = new Tuple();
            t.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            t.setIntFld(1,LSHFLeafPage.pageType);

            Tuple t2 = new Tuple();
            t2.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            t2.setIntFld(1,0);

            this.insertRecord(t.getTupleByteArray());
            this.insertRecord(t2.getTupleByteArray());
        } catch (Exception e){
            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
        }
    }

    //    public LSHFLeafPage(Page page){
//        super(page);
//    }
    public LSHFLeafPage(PageId pageId, String indexName) throws ConstructPageException, IOException {
        super(indexName);
        this.pageId = pageId;
        try{
            super.setCurPage(pageId);
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
//            this.init(pageId, this);
//            System.out.println("next page leaf "+this.getNextPage()+","+pageId.pid);
//            System.out.println("LSHFLeafPage init");
            //
        }
        catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHLeafPage");
        }
    }

    public LSHFLeafPage(LSHBasePage basePage, String indexName) throws ConstructPageException, IOException {
        super(indexName);

        this.pageId = basePage.getCurPage();
        try{
            this.setCurPage(basePage.getCurPage());
            this.setpage(basePage.getpage());
        }
        catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHLeafPage");
        }
    }

    private int getNumVectorInPage() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.getCurPage(), 1));
        t.setHdr((short)2, new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(2);
    }
    private void incrNumVectorInPage() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.getCurPage(), 1));

        t.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        t.setIntFld(1,t.getIntFld(1)+1);
        // need to write this in slot 0... // write a function in hfPage to the same....

    }


    public void insert(Vector100Dtype v, RID rid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, ConstructPageException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

        if(this.available_space()<250){ // use this or increment num vectors and use that
            LSHFLeafPage l = null;
            if (this.getNextPage().pid != INVALID_PAGE){
                l = new LSHFLeafPage(this.getNextPage(), this.indexName);
            }
            else {
                l = new LSHFLeafPage(this.indexName);
                this.setNextPage(l.getCurPage());
            }
            l.insert(v, rid);
            SystemDefs.JavabaseBM.unpinPage(l.getCurPage(),true);
        }
        else {
            Tuple t = new Tuple();
            t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            t.set100DVectFld(1,v);
            t.setIntFld(2,rid.pageNo.pid);
            t.setIntFld(3,rid.slotNo);
            this.insertRecord(t.getTupleByteArray());
//            this.incrNumVectorInPage();
        }
    }

    public int delete(Vector100Dtype v, boolean basePage, Heapfile hf) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidSlotNumberException, ConstructPageException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidRunSizeException, InvalidPageNumberException, FileIOException, DiskMgrException, SpaceNotAvailableException, HFDiskMgrException, HFException, HFBufMgrException {
        int npPresent = 0;
        if (this.getNextPage().pid != INVALID_PAGE){
            npPresent = 1;
        }
        List<RID> deletedRids = new ArrayList<>();
        for(int i = 2;i<this.getSlotCnt();i++){
            RID rid = new RID(this.getCurPage(), i);
            Tuple t = this.getRecord(rid);
            t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            int s  = t.getIntFld(3);
            Vector100Dtype vector100Dtype = t.get100DVectFld(1);
            // vector100Dtype.print();
            if (t.get100DVectFld(1).distanceTo(v) == 0){
                hf.insertRecord(t.getTupleByteArray());
//                this.deleteRecord(rid);
                deletedRids.add(rid);
//                if (!basePage){
////                    if (this.getSlotCnt() + npPresent == 2){
////                        SystemDefs.JavabaseDB.deallocate_page(this.getCurPage());
////                    }
//                    SystemDefs.JavabaseBM.unpinPage(this.getCurPage(),true); // should this be in the else?
//                }
//                return this.getSlotCnt() + npPresent - 2;
            }
        }

        for (RID rid : deletedRids){
            this.deleteRecord(rid);
        }
        this.compact_slot_dir();

        if (!basePage){
            SystemDefs.JavabaseBM.unpinPage(this.getCurPage(),true);
        }


        PageId nextPageId = this.getNextPage();

        if (nextPageId.pid != INVALID_PAGE){
            LSHFLeafPage l = new LSHFLeafPage(this.getNextPage(), this.indexName);
            int d =  l.delete(v, false, hf);
            if (d == 0 ){
                nextPageId = l.getNextPage();

                // pin again just to set the next page
                LSHFLeafPage presCopy = new LSHFLeafPage(this.getCurPage(), this.indexName);
                presCopy.setNextPage(nextPageId);
                SystemDefs.JavabaseBM.unpinPage(presCopy.getCurPage(),true);


                SystemDefs.JavabaseDB.deallocate_page(l.getCurPage());
            }
        }

//        if (!basePage){
//                SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
//        }

        return this.getSlotCnt() + npPresent -2;
    }

    public LSHDto getFirstVector() throws InvalidTupleSizeException, IOException, InvalidTypeException, InvalidSlotNumberException, FieldNumberOutOfBoundException {
        if (this.getSlotCnt()<=2){
            return null;
        }
        Tuple t = this.getRecord(new RID(this.getCurPage(), 2));
        t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        return new LSHDto(t.get100DVectFld(1),t.getIntFld(2), t.getIntFld(3));
    }


    @Override
    public Iterator<LSHDto> iterator() {
        try {
            return new LSHLeafPageIterator(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class LSHLeafPageIterator implements Iterator<LSHDto> {

        private LSHFLeafPage leafPage;
        private final int baseLeafPageId;
        private int index;

        public LSHLeafPageIterator(LSHFLeafPage leafPage) throws IOException {
            this.leafPage = leafPage;
            this.index = 2;
            this.baseLeafPageId = leafPage.getCurPage().pid;
        }
        @Override
        public boolean hasNext() {
            try {
                if (leafPage== null){
                    return false;
                }
                if (index >= 0 && index < leafPage.getSlotCnt() ){
                    return true;
                }
                if (leafPage.getNextPage().pid != INVALID_PAGE){
                    return true;
                }
                if(leafPage.getCurPage().pid != baseLeafPageId){
                    SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
                }
                this.leafPage = null;
                this.index = -1;
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (HashEntryNotFoundException e) {
                throw new RuntimeException(e);
            } catch (InvalidFrameNumberException e) {
                throw new RuntimeException(e);
            } catch (PageUnpinnedException e) {
                throw new RuntimeException(e);
            } catch (ReplacerException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public LSHDto next() {
            try {
                if (index < leafPage.getSlotCnt()){
                    Tuple t = this.leafPage.getRecord(new RID(this.leafPage.getCurPage(), index));
                    t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
                    int pgid = t.getIntFld(2);
                    int slotNum = t.getIntFld(3);
                    Vector100Dtype v = t.get100DVectFld(1);
                    this.index = this.index + 1;
                    return new LSHDto(v,pgid, slotNum);
                }
                else if (leafPage.getNextPage().pid != INVALID_PAGE){
                    LSHFLeafPage nl = new LSHFLeafPage(this.leafPage.getNextPage(), this.leafPage.indexName);
                    if (this.leafPage.getCurPage().pid != baseLeafPageId) {
                        SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
                    }
                    this.leafPage = nl;
                    index = 2;
                    return next();
                }
                else if (leafPage.getNextPage().pid == INVALID_PAGE){
                    if (this.leafPage.getCurPage().pid != baseLeafPageId){
                        SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
                    }
                    this.leafPage = null;
                    this.index = -1;
                }
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
            } catch (HashEntryNotFoundException e) {
                throw new RuntimeException(e);
            } catch (InvalidFrameNumberException e) {
                throw new RuntimeException(e);
            } catch (ConstructPageException e) {
                throw new RuntimeException(e);
            } catch (PageUnpinnedException e) {
                throw new RuntimeException(e);
            } catch (ReplacerException e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }
}

/*
tuple of vector, (page id, slot id) -> 218bytes -> 200 + 4 + 4 + 2*3 + (4)
max number of vectors to fit in a page = 18 considering page size is 4096B -> should this be hardcoded?
 */
/*
 (leaf page1 > 18v -> overflow leaf page1 -> overflow page 2) -> 1 bucket
    (pageType)(total number of vectors in the page), [(vector key, rid), (....), ....]
 */