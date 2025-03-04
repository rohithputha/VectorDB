package LSHFIndex;

import btree.ConstructPageException;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;

public class LSHFLeafPage extends HFPage{
    private PageId pageId;
    public static int PageType = 4;

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
            //changes made here--> adding extra fld to store type
            Tuple t = new Tuple();
            t.setHdr((short)2, new AttrType[]{
                new AttrType(AttrType.attrInteger), // Field 1: type (0 for leaf)
                new AttrType(AttrType.attrInteger)  // Field 2: numVectors
            }, null);
            t.setIntFld(1, 0); // Type: 0 for leaf
            t.setIntFld(2, 0);
            this.insertRecord(t.getTupleByteArray());
        } catch (Exception e){
            throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
        }
    }

    public LSHFLeafPage(Page page){
        super(page);
    }
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

    // Method to get page type from metadata tuple
    public short getPageType() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.getCurPage(), 0));
        t.setHdr((short)2, new AttrType[]{
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
        }, null);
        return (short) t.getIntFld(1); // Field 1 is type
    }

    

    private int getNumVectorInPage() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
       Tuple t = this.getRecord(new RID(this.getCurPage(), 0));
       t.setHdr((short)2, new AttrType[]{
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrInteger)
    }, null);
    return t.getIntFld(2); // Field 2 is numVectors
    }
    private void incrNumVectorInPage() throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = this.getRecord(new RID(this.getCurPage(), 0));
        t.setHdr((short)2, new AttrType[]{
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
        }, null);
        t.setIntFld(2, t.getIntFld(2) + 1); // Increment numVectors
        this.deleteRecord(new RID(this.getCurPage(), 0)); // Remove old tuple
        this.insertRecord(t.getTupleByteArray()); // Insert updated tuple
    }
    public void insert(Vector100Dtype v, RID rid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, ConstructPageException, InvalidTupleSizeException, InvalidTypeException {
        // should there be a check if the key is already present on the leaf page?
        if(this.getNumVectorInPage() == 18){ // 18 is the max number of vectors that can fit into a leaf page
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

    

}

/*
max number of vectors to fit in a page = 18 considering page size is 4096B -> should this be hardcoded?
 */
/*
    (total number of vectors in the page), [(vector key, rid), (....), ....]
 */