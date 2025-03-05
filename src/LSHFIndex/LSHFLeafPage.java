
package LSHFIndex;

import btree.ConstructPageException;
import diskmgr.Page;
import global.*;
import heap.*;
import org.w3c.dom.Attr;

import java.io.IOException;

public class LSHFLeafPage extends LSHBasePage{
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
        // should there be a check if the key is already present on the leaf page?
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
            t.setVector100DFld(1,v);
            t.setIntFld(2,rid.pageNo.pid);
            t.setIntFld(3,rid.slotNo);
            this.insertRecord(t.getTupleByteArray());
            this.incrNumVectorInPage();
        }
    }
}

/*
max number of vectors to fit in a page = 18 considering page size is 4096B -> should this be hardcoded?
 */
/*
    (pageType)(total number of vectors in the page), [(vector key, rid), (....), ....]
 */