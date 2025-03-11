package LSHFIndex;

import btree.ConstructPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.Iterator;

public class LSHFLeafPage extends LSHBasePage implements Iterable<LSHDto> {
    private PageId pageId;
    public static final int pageType = 4;
    public LSHFLeafPage() throws ConstructPageException {
        super();
        try{
            Page newPage = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
            if (newPageId == null){
                throw new ConstructPageException(null, "new page failed. failed to construct LSHHeaderPage");
            }
            this.init(newPageId, newPage); // this is where it gets associated with the current page
            this.pageId = newPageId;
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
    public LSHFLeafPage(PageId pageId) throws ConstructPageException {
        super();
        super.curPage = pageId;
        this.pageId = pageId;
        try{
            SystemDefs.JavabaseBM.pinPage(pageId, this, false); //
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHLeafPage");
        }
    }

    public LSHFLeafPage(LSHBasePage basePage) throws ConstructPageException, IOException {
        super();
        super.curPage = basePage.getCurPage();
        this.pageId = basePage.getCurPage();
        try{
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
            SystemDefs.JavabaseBM.unpinPage(pageId, false);
        } catch (Exception e) {
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


    public void insert(Vector100Dtype v, RID rid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, ConstructPageException, InvalidTupleSizeException, InvalidTypeException {
        // fix this: we are not going to the end of the list of overflow pages, we are just checking the base leaf page available space which is a mistake.,
        if(this.available_space()<250){ // use this or increment num vectors and use that
            LSHFLeafPage l = null;
            if (this.getNextPage().pid != INVALID_PAGE){
                l = new LSHFLeafPage(this.getNextPage());
            }
            else {
                l = new LSHFLeafPage();
                this.setNextPage(l.getCurPage());
            }
            l.insert(v, rid);
        }
        else {
            Tuple t = new Tuple();
            t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            t.set100DVectFld(1,v);
            t.setIntFld(2,rid.pageNo.pid);
            t.setIntFld(3,rid.slotNo);
            this.insertRecord(t.getTupleByteArray());
            this.incrNumVectorInPage();
        }
    }


    @Override
    public Iterator<LSHDto> iterator() {
        return new LSHLeafPageIterator(this);
    }

    private class LSHLeafPageIterator implements Iterator<LSHDto> {

        private LSHFLeafPage leafPage;
        private int index;

        public LSHLeafPageIterator(LSHFLeafPage leafPage) {
            this.leafPage = leafPage;
            this.index = 2;
        }
        @Override
        public boolean hasNext() {
            try {
                if (leafPage== null){
                    return false;
                }
                if (index < leafPage.getSlotCnt()){
                    return true;
                }
                if (leafPage.getNextPage().pid != INVALID_PAGE){
                    return true;
                }
                if(leafPage != null || index != -1){
                    SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
                    this.leafPage = null;
                    this.index = -1;
                }
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
                    this.index+=1;
                    return new LSHDto(v,pgid, slotNum);
                }
                else if (leafPage.getNextPage().pid != INVALID_PAGE){
                    LSHFLeafPage nl = new LSHFLeafPage(this.leafPage.getCurPage());
                    SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
                    this.leafPage = nl;
                    index = 2;
                    return next();
                }
                else if (leafPage.getNextPage().pid == INVALID_PAGE){
                    SystemDefs.JavabaseBM.unpinPage(this.leafPage.getCurPage(), false);
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