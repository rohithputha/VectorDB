package LSHFIndex;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class LSHIndexFileTest {
    public static void main(String[] args) throws HashEntryNotFoundException, BufferPoolExceededException, PageNotReadException, ConstructPageException, HashOperationException, BufMgrException, InvalidRunSizeException, GetFileEntryException, PagePinnedException, InvalidFrameNumberException, IOException, OutOfSpaceException, FieldNumberOutOfBoundException, FileNameTooLongException, InvalidPageNumberException, FileIOException, DuplicateEntryException, PageUnpinnedException, DiskMgrException, ReplacerException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException, PageNotFoundException {
        String dbpath = "testdb1";
        int num_pages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
        SystemDefs sysdef = new SystemDefs(dbpath, num_pages, num_pages, "Clock");
        System.out.println("System defs: " + sysdef);
        Page page = new Page();
        SystemDefs.JavabaseDB.read_page(new PageId(2),page);
        System.out.println(page.getpage());
        System.out.println("MiniBase initialized with " + num_pages + " buffer pages");

        LSHFIndexFile indexFile  = new LSHFIndexFile("lshindex",10,10);
        indexFile.close();
        indexFile = new LSHFIndexFile("lshindex");
//        LSHFIndexFile indexFile = new LSHFIndexFile("lshindex");
        indexFile.insert(new Vector100Dtype(), new RID(new PageId(1),1));
        SystemDefs.JavabaseDB.read_page(new PageId(2),page);
        SystemDefs.JavabaseDB.closeDB();

        int a[] = new int[0];
//        indexFile.insert(new Vector100Dtype(),new RID(new PageId(1),2));
    }
}
