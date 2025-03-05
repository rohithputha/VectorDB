package LSHFIndex;

import heap.HFPage;
import global.*;
import java.io.*;
import bufmgr.*;

public class LSHFPage extends HFPage {
    // Instance variable for page type, set by subclasses
    public static int getPageType;

    // Default constructor
    public LSHFPage() {
        super();
    }

    // Constructor for pinning an existing page
//    public LSHFPage(PageId pageId) throws IOException,ReplacerException, BufferPoolExceededException, BufMgrException, HashEntryNotFoundException, HashOperationException, InvalidBufferException, InvalidFrameNumberException, PageNotFoundException, PageNotReadException, PagePinnedException, PageUnpinnedException {
//        super();
//        this.setCurPage(pageId); // Set current page ID
//        SystemDefs.JavabaseBM.pinPage(pageId, this, false);
//    }
}