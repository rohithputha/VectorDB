package LSHFIndex;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LSHFIndexFile implements LSHIndexFileInterface, GlobalConst {
    private static FileOutputStream fileOutputStream;
    private static DataOutputStream dataOutputStream;

    private LSHHeaderPage headerPage;
    private PageId headerPageId;
    private String fileName;
    private int h;
    private int L;


    private static PageId getHeaderPageId(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e.getMessage());
        }
    }


    // this means the index file is new and the header pages have to be created
    public LSHFIndexFile(String fileName, int h, int l) throws GetFileEntryException, InvalidPageNumberException, IOException, FileIOException, DiskMgrException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, ConstructPageException, HashEntryNotFoundException, BufferPoolExceededException, PageNotReadException, FieldNumberOutOfBoundException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException, PageNotFoundException {
        PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
        if (headerPageId == null) {
            LSHHeaderPage headerPage = LSHHeaderPage.LSHHeaderPageFactory.createHeaderPages(h, l);
            if (headerPage == null) {
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

            LSHashFunctionsMap lshhash = LSHashFunctionsMap.getInstance();
            LSHLayerMap lshLayerMap = LSHLayerMap.getInstance();
            System.out.println(lshhash);
        } else {
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
        LSHashFunctionsMap functionsMap = LSHashFunctionsMap.getInstance();
        this.headerPage.getLayers(h);
        LSHLayerMap layerMap = LSHLayerMap.getInstance();

        this.fileName = fileName;
    }

    private void _insertOnEachLayer(int startPageId, Vector100Dtype key, RID rid) throws FieldNumberOutOfBoundException, InvalidSlotNumberException, IOException, ConstructPageException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        LSHFInnerPage startPage = new LSHFInnerPage(new PageId(startPageId));
        LSHFInnerPage currentPage = startPage;
        LSHFInnerPage prevPage = null;
        LSHFLeafPage leafPageFound = null;
        do {
            int hashInConsideration = currentPage.getHashFunctionInConsideration();

            int fPid = currentPage.getBucketByKey(key);
            if (fPid == -1) {
                // the bucket is not found. create a new innerpage and insert the pid for this bucket and then go to that page.
                // additional checks to be present if the it is last of hashes
                if (hashInConsideration == this.h - 1) {
                    // create a leaf page
                    leafPageFound = new LSHFLeafPage();
                    currentPage.insertBucketByKey(key, leafPageFound.getCurPage().pid);
                } else {
                    //create an inner page
                    LSHFInnerPage newPage = new LSHFInnerPage(startPage.getLayerId(), hashInConsideration + 1);
                    currentPage.insertBucketByKey(key, newPage.getCurPage().pid);
                    prevPage = currentPage;
                    currentPage = newPage;
                    SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), true);
                }

            } else {
                PageId fPageId = new PageId(fPid);
                if (hashInConsideration == this.h - 1) {
                    leafPageFound = new LSHFLeafPage(fPageId);
                } else {
                    prevPage = currentPage;
                    currentPage = new LSHFInnerPage(fPageId);
                    SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), false);
                }
            }
        } while (leafPageFound == null);

        if (leafPageFound == null) {
            // error
            // throw an error
            return; // this can be removed if error throw
        }
        leafPageFound.insert(key, rid);
        SystemDefs.JavabaseBM.unpinPage(leafPageFound.getCurPage(), false);
    }

    @Override
    public void insert(Vector100Dtype key, RID rid) throws FieldNumberOutOfBoundException, ConstructPageException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        LSHLayerMap layerMap = LSHLayerMap.getInstance();
        Iterator<LSHLayer> iter = layerMap.iterator();
        while (iter.hasNext()) {
            LSHLayer layer = iter.next();
            this._insertOnEachLayer(layer.getLayerStartPage(), key, rid);
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

    public List<LSHDto> collectLeafPageIds(LSHBasePage currentPage) throws Exception {
        List<LSHDto> leafPageIds = new ArrayList<>();

        // Get the current hash function index
        //int hashFuncIndex = currentPage.getHashFunctionInConsideration();

        // If this is the last hash function, collect leaf page IDs
        if (currentPage.getPageType() == LSHFLeafPage.pageType) {
            // Iterate through all slots in the current page
            Iterator<LSHDto> lshLeafDtoIterator = (new LSHFLeafPage(currentPage)).iterator();
            while (lshLeafDtoIterator.hasNext()) {
                leafPageIds.add(lshLeafDtoIterator.next());
            }
            return leafPageIds;
        } else if (currentPage.getPageType() == LSHFInnerPage.pageType) {
            LSHFInnerPage currentInnerPage = new LSHFInnerPage(currentPage);
            for (short i = 2; i < currentInnerPage.getSlotCnt(); i++) {
                Tuple t = currentInnerPage.getRecord(new RID(currentInnerPage.getCurPage(), i));
                t.setHdr((short) 2,
                        new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                        null);

                // Get the page ID for the next level
                int nextPageId = t.getIntFld(2);
                // Recursively process the next inner page
                LSHBasePage nextPage = new LSHBasePage(new PageId(nextPageId));
                leafPageIds.addAll(collectLeafPageIds(nextPage));
            }
            SystemDefs.JavabaseBM.unpinPage(currentInnerPage.getCurPage(), false);
        }

        // If not the last hash function, recursively process buckets
        return leafPageIds;
    }

    private List<LSHDto> _nearestNeighboursEachLayer(int currentPageId,Vector100Dtype v, int k, LSHLayer layer) throws ConstructPageException, FieldNumberOutOfBoundException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId));
        List<LSHDto> a = new ArrayList<>();
        if(basePage.getPageType() == LSHFInnerPage.pageType){
            LSHFInnerPage innerPage = new LSHFInnerPage(basePage);
            int pageId = innerPage.getBucketByKey(v);
            a = _nearestNeighboursEachLayer(pageId,v,k,layer);
            if (a.size() < k) {
                Iterator<List<LSHDto>> iterator = innerPage.expansionIterator(pageId);
                while (iterator.hasNext() && a.size() < k) {
                    List<LSHDto> next = iterator.next();
                    for (LSHDto d : next) {
                        a.addAll(_nearestNeighboursEachLayer(d.getPid(), v, k - a.size(), layer));
                    }
                }
            }
            SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
        }
        else if (basePage.getPageType() == LSHFLeafPage.pageType){
            LSHFLeafPage leafPage = new LSHFLeafPage(basePage);
            Iterator<LSHDto> iterator = leafPage.iterator();
            while(iterator.hasNext()){
                a.add(iterator.next());
            }
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
        }
        return a;
    }

    public List<LSHDto> nearestNeighbourScan(Vector100Dtype v, int k) throws Exception {
        LSHLayerMap layerMap = LSHLayerMap.getInstance();
        Iterator<LSHLayer> iter = layerMap.iterator();
        List<LSHDto> a = new ArrayList<>();

        while (iter.hasNext()) {
            LSHLayer layer = iter.next();
            a.addAll(this._nearestNeighboursEachLayer(layer.getLayerStartPage(), v, k, layer));
        }
        // right now returns more than k, need a top k choosing logic
        return a;
    }

}