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
import heap.HFPage;
import heap.Tuple;
import heap.*;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class LSHFIndexFile implements LSHIndexFileInterface, GlobalConst {
    private static FileOutputStream fileOutputStream;
    private static DataOutputStream dataOutputStream;

    private LSHHeaderPage headerPage;
    private PageId headerPageId;
    private String fileName;
    private int h;
    private int L;


    private static PageId getHeaderPageId(String fileName) throws GetFileEntryException {
        try{
            return SystemDefs.JavabaseDB.get_file_entry(fileName);
        } catch(Exception e){
            e.printStackTrace();
            throw new GetFileEntryException(e.getMessage());
        }
    }


    // this means the index file is new and the header pages have to be created
    public LSHFIndexFile(String fileName, int h, int l) throws GetFileEntryException, InvalidPageNumberException, IOException, FileIOException, DiskMgrException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, ConstructPageException, HashEntryNotFoundException, BufferPoolExceededException, PageNotReadException, FieldNumberOutOfBoundException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException, PageNotFoundException {
            PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
            if (headerPageId == null) {
                LSHHeaderPage headerPage = LSHHeaderPage.LSHHeaderPageFactory.createHeaderPages(h, l);
                if(headerPage == null) {
                    throw new ConstructPageException(null, "LSH header page could not be constructed. Index file could not be created.");
                }
                headerPageId = headerPage.getCurPage();
                SystemDefs.JavabaseDB.add_file_entry(fileName, headerPageId);

                PageId t = SystemDefs.JavabaseDB.get_file_entry(fileName);
                assert t != null;
                this.headerPageId = headerPageId;
                this.headerPage = headerPage;
                this.h = h;
                this.L = l;

                //for (int i = 0; i < l; i++) {
                    // Allocate a start page for the layer
                //    Page newPage = new Page();
                //    PageId startPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
                //    if (startPageId == null) {
                //        throw new IOException("Failed to allocate start page for layer " + i);
                //    }
                    // Simple hash function indices (e.g., [0, 1] for layer 0, [2, 3] for layer 1)
                //    int[] hashFunctions = new int[h];
                //   for (int j = 0; j < h; j++) {
                //        hashFunctions[j] = i * h + j; // Unique indices per layer
                //    }
                //    LSHLayer layer = new LSHLayer(hashFunctions, startPageId.pid, i);
                //    LSHLayerMap.addLayer(layer);
                //}

                LSHashFunctionsMap lshhash = LSHashFunctionsMap.getInstance();
                LSHLayerMap lshLayerMap = LSHLayerMap.getInstance();
//                System.out.println(lshhash);
            }
            else {
                this.headerPage = new LSHHeaderPage(headerPageId);
            }
            this.fileName = fileName;

    }

    //this means this is an old index file and the header pages have already been created
    public LSHFIndexFile(String fileName) throws GetFileEntryException, InvalidPageNumberException, IOException, FileIOException, DiskMgrException, ConstructPageException, BufferPoolExceededException, HashEntryNotFoundException, PageNotReadException, FieldNumberOutOfBoundException, HashOperationException, BufMgrException, InvalidSlotNumberException, PagePinnedException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
        PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
        if (headerPageId == null) {
            throw new GetFileEntryException(fileName);
        }
        this.headerPageId = headerPageId;
        this.headerPage = new LSHHeaderPage(headerPageId);// this should construct the layer and hash functions map...
        this.headerPage.getHashFunctions();
        this.h = this.headerPage.getHashFunctionsPerLayer();
        LSHashFunctionsMap.getInstance();
        this.headerPage.getLayers(h);
        LSHLayerMap.getInstance();

        this.fileName = fileName;
    }

    private void _insertOnEachLayer(int startPageId, Vector100Dtype key, RID rid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, ConstructPageException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException {
        HFPage currentPage = new LSHFInnerPage(new PageId(startPageId)); // Start with inner page
    HFPage prevPage = null;
    LSHFLeafPage leafPageFound = null;

    do {
        short type;
        if (currentPage instanceof LSHFLeafPage) {
            type = ((LSHFLeafPage) currentPage).getPageType();
        } else if (currentPage instanceof LSHFInnerPage) {
            type = ((LSHFInnerPage) currentPage).getPageType();
        } else {
            throw new IOException("Unexpected page instance type");
        }

        if (type == 4) { // Leaf page
            leafPageFound = new LSHFLeafPage(currentPage.getCurPage());
        } else if (type == 5) { // Inner page
            LSHFInnerPage innerPage = (LSHFInnerPage) currentPage;
            int hashInConsideration = innerPage.getHashFunctionInConsideration();
            int fPid = innerPage.getBucketByKey(key);
            if (fPid == -1) {
                if (hashInConsideration == this.h - 1) {
                    leafPageFound = new LSHFLeafPage();
                    innerPage.insertBucketByKey(key, leafPageFound.getCurPage().pid);
                } else {
                    LSHFInnerPage newPage = new LSHFInnerPage(innerPage.getLayerId(), hashInConsideration + 1);
                    innerPage.insertBucketByKey(key, newPage.getCurPage().pid);
                    prevPage = currentPage;
                    currentPage = newPage;
                    if (prevPage != null) {
                        SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), true);
                    }
                }
            } else {
                prevPage = currentPage;
                currentPage = getPageById(new PageId(fPid)); // Fetch next page
                if (prevPage != null) {
                    SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), true);
                }
            }
        } else {
            if (prevPage != null) {
                SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), false);
            }
            SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), false);
            throw new IOException("Unknown page type: " + type);
        }
    } while (leafPageFound == null);

    if (leafPageFound == null) {
        System.err.println("Failed to find or create a leaf page for insertion");
        return;
    }
    leafPageFound.insert(key, rid);
    if (currentPage != null && currentPage != leafPageFound) {
        SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), true);
    }
}

    
    // Helper method to get page type from metadata tuple
    private short getPageType(HFPage page) throws IOException, InvalidSlotNumberException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        Tuple t = page.getRecord(new RID(page.getCurPage(), 0));
        if (t == null) {
            throw new IOException("No metadata tuple found in page " + page.getCurPage().pid);
        }
        try {
            t.setHdr((short)2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            return (short) t.getIntFld(1); // Leaf page: [type, numVectors]
        } catch (Exception e) {
            t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            return (short) t.getIntFld(1); // Inner page: [type, hashInConsideration, layerId]
        }
    }
    
    // Helper method to fetch a page by ID and instantiate the correct subclass
    private HFPage getPageById(PageId pageId) throws IOException, ConstructPageException, ReplacerException, InvalidSlotNumberException, HashOperationException, PageUnpinnedException, FieldNumberOutOfBoundException, InvalidFrameNumberException, InvalidTupleSizeException, HashEntryNotFoundException, PageNotReadException, InvalidTypeException, BufferPoolExceededException, PagePinnedException, BufMgrException {
        HFPage page = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        short type = getPageType(page);
        if (type == 0) {
            return new LSHFLeafPage(pageId);
        } else if (type == 1) {
            return new LSHFInnerPage(pageId);
        } else {
            SystemDefs.JavabaseBM.unpinPage(pageId, false);
            throw new IOException("Unknown page type: " + type);
        }
    }

    @Override
    public void insert(Vector100Dtype key, RID rid) throws FieldNumberOutOfBoundException, ConstructPageException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException{
        LSHLayerMap layerMap = LSHLayerMap.getInstance();
        Iterator<LSHLayer> iter =  layerMap.iterator();
        while(iter.hasNext()){
            LSHLayer layer = iter.next();
            this._insertOnEachLayer(layer.getLayerStartPage(),key, rid);
        }

    }

    public void close()
            throws PageUnpinnedException,
            InvalidFrameNumberException,
            HashEntryNotFoundException,
            ReplacerException, PageNotFoundException, HashOperationException, BufMgrException, PagePinnedException, IOException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            SystemDefs.JavabaseBM.flushAllPages();
            headerPage = null;
        }
    }
    


}
