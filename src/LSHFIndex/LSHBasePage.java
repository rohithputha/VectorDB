package LSHFIndex;

import btree.ConstructPageException;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class LSHBasePage extends HFPage {
    public  int getPageType() throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException {
        Tuple t = this.getRecord(new RID(this.getCurPage(), 0 ));
        t.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
        return t.getIntFld(1);
    }
    public LSHBasePage() {
        super();
    }

    public LSHBasePage(PageId pageId) throws ConstructPageException {
        super();
        super.curPage = pageId;
        try{
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pin page failed. failed to construct LSHBasePage");
        }

    }
}
