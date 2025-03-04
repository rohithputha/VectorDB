package LSHFIndex;

import btree.ConstructPageException;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import bufmgr.HashOperationException;
import global.RID;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public interface LSHIndexFileInterface {
    public void insert(Vector100Dtype key, RID rid) throws FieldNumberOutOfBoundException, ConstructPageException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException;

}
