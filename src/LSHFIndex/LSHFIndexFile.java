package LSHFIndex;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

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

    // l -> compound hash -> (h1, h2, h3,... hn)-> n-> h
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

    private int _nearestNeighboursEachLayer(int currentPageId, Vector100Dtype v, int k, LSHLayer layer, Heapfile hf, boolean considerAll, Set<Integer> heapFilePages, Set<String> rids) throws ConstructPageException, FieldNumberOutOfBoundException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, SpaceNotAvailableException, HFDiskMgrException, HFException, HFBufMgrException {

        LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId));
        int a = 0;
        if(basePage.getPageType() == LSHFInnerPage.pageType){
            if (!considerAll) {
                LSHFInnerPage innerPage = new LSHFInnerPage(basePage);
                int pageId = innerPage.getBucketByKey(v);
                a = _nearestNeighboursEachLayer(pageId,v,k,layer, hf, false, heapFilePages, rids);
                if (a < k) {
                    LSHFInnerPage.LSHInnerPageIterator iterator = innerPage.iterator();
                    iterator.setExceptions(pageId);
                    while (iterator.hasNext() && a < k) {
                        LSHDto next = iterator.next();
                        if (next.getPid()== -1){
                            continue;
                        }
                        a += _nearestNeighboursEachLayer(next.getPid(), v, k - a, layer, hf, true, heapFilePages, rids);
                    }
                }
                SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
            }
            else{
                LSHFInnerPage innerPage = new LSHFInnerPage(basePage);
                LSHFInnerPage.LSHInnerPageIterator iterator = innerPage.iterator();
                while (iterator.hasNext() && a < k) {
                    LSHDto next = iterator.next();
                    if (next.getPid()== -1){
                        continue;
                    }
                    a += _nearestNeighboursEachLayer(next.getPid(), v, k - a, layer, hf, true, heapFilePages, rids);
                }
                SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
            }

        }
        else if (basePage.getPageType() == LSHFLeafPage.pageType){
            LSHFLeafPage leafPage = new LSHFLeafPage(basePage);
            Iterator<LSHDto> iterator = leafPage.iterator();
            while(iterator.hasNext()){
                LSHDto next = iterator.next();
                if (!rids.contains(next.getPid()+"_"+next.getSid())) {
                    RID rid = hf.insertRecord(next.toLeafTuple().getTupleByteArray());
                    a = a + 1;
                    heapFilePages.add(rid.pageNo.pid);
                    rids.add(next.getPid()+ "_" + next.getSid());
                }
            }
//            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false); -> some weird error is being thrown here-> check why?
        }
        return a;
    }

    public Sort nearestNeighbourScan(Vector100Dtype v, int k) throws Exception {
        LSHLayerMap layerMap = LSHLayerMap.getInstance();
        Iterator<LSHLayer> iter = layerMap.iterator();
        String tempFileName = v.toString()+k+"nn_temp.heap";
        Heapfile tempHf = new Heapfile(tempFileName);
        int totalCollected = 0;
        Set<Integer> heapFilePages = new HashSet<>();
        Set<String> rids = new HashSet<>();
        while (iter.hasNext()) {
            LSHLayer layer = iter.next();
            totalCollected += this._nearestNeighboursEachLayer(layer.getLayerStartPage(), v, k, layer, tempHf, false, heapFilePages, rids);
        }
        FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
        AttrType[] types = {new AttrType(AttrType.attrVector100D), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
        FileScan fs = new FileScan(tempFileName, types, null, (short)3, (short)3, projlist, null);
        return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200,heapFilePages.size(), v, k);
        // chaneg the number of heap file pages to scan
        // also need to remove duplicate records for nn scan

    }

    // private void _rangeScanEachLayer(int currentPageId, Vector100Dtype v, int distance, LSHLayer layer, Heapfile hf, Set<Integer> heapFilePages, Set<String> rids) throws ConstructPageException, FieldNumberOutOfBoundException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException {
        
    //     LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId));
    //     List<LSHDto> a = new ArrayList<>();
    //     if(basePage.getPageType() == LSHFInnerPage.pageType){
    //         LSHFInnerPage innerPage = new LSHFInnerPage(basePage);
    //         int pageId = innerPage.getBucketByKey(v);
    //         if (pageId != -1){
    //             _rangeScanEachLayer(pageId, v, distance, layer, hf, heapFilePages, rids);
    //         }
    //         LSHFInnerPage.LSHInnerPageIterator iterator = innerPage.iterator();
    //         while (iterator.hasNext()) {
    //             LSHDto next = iterator.next();
    //             if (next.getPid()== -1){
    //                 continue;
    //             }
    //             _rangeScanEachLayer(next.getPid(), v, distance, layer, hf, heapFilePages, rids);
    //         }
    //         SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
    //     }
    //     else if (basePage.getPageType() == LSHFLeafPage.pageType){
    //         LSHFLeafPage leafPage = new LSHFLeafPage(basePage);
    //         Iterator<LSHDto> iterator = leafPage.iterator();
    //         while(iterator.hasNext()){
    //             LSHDto next = iterator.next();
    //             int dist = v.distanceTo(next.getV());
    //             if (dist <= distance){
    //                 String ridKey = next.getPid() + "_" + next.getSid();
    //                 if (!rids.contains(ridKey)) {
    //                     RID rid = hf.insertRecord(next.toLeafTuple().getTupleByteArray());
    //                     heapFilePages.add(rid.pageNo.pid);
    //                     rids.add(ridKey);
    //                 }
    //             }
    //         }
    //         // SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
    //     }
    // }

    // public Sort rangeScan(Vector100Dtype v, int distance) throws Exception {
    //     LSHLayerMap layerMap = LSHLayerMap.getInstance();
    //     Iterator<LSHLayer> iter = layerMap.iterator();
    //     String tempFileName = v.toString()+distance+"range_temp.heap";
    //     Heapfile tempHf = new Heapfile(tempFileName);
    //     Set<Integer> heapFilePages = new HashSet<>();
    //     Set<String> rids = new HashSet<>();

    //     while (iter.hasNext()) {
    //         LSHLayer layer = iter.next();
    //         layer.getLayerStartPage();
    //         this._rangeScanEachLayer(layer.getLayerStartPage(), v, distance, layer, tempHf, heapFilePages, rids);
    //     }
    //     FldSpec[] projlist = {
    //         new FldSpec(new RelSpec(RelSpec.outer), 1),
    //         new FldSpec(new RelSpec(RelSpec.outer), 2), 
    //         new FldSpec(new RelSpec(RelSpec.outer), 3)
    //         };
    //     AttrType[] types = {
    //         new AttrType(AttrType.attrVector100D), 
    //         new AttrType(AttrType.attrInteger), 
    //         new AttrType(AttrType.attrInteger)
    //         };
    //     FileScan fs = new FileScan(tempFileName, types, null, (short)3, (short)3, projlist, null);
    //     return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200, heapFilePages.size(), v, rids.size());
    // }



    private boolean _rangeScanEachLayer(int currentPageId, Vector100Dtype v, 
        int distance, LSHLayer layer, Heapfile hf, Set<Integer> heapFilePages, 
        Set<String> rids, double notMatchingLimit) 
        throws ConstructPageException, FieldNumberOutOfBoundException, 
        InvalidSlotNumberException, InvalidTupleSizeException, IOException, 
        InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, 
        PageUnpinnedException, ReplacerException, SpaceNotAvailableException, HFException, 
        HFBufMgrException, HFDiskMgrException {
            LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId));
            boolean scanNext = true;
            if(basePage.getPageType() == LSHFInnerPage.pageType){
                LSHFInnerPage innerPage = new LSHFInnerPage(basePage);
                int hashIndex = innerPage.getHashFunctionInConsideration();
                int pageId = innerPage.getBucketByKey(v);
                if (pageId != -1){
                    scanNext = _rangeScanEachLayer(pageId, v, distance, layer, hf, heapFilePages, rids, notMatchingLimit);
                    if (!scanNext){
                        SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
                        return false;
                    }
                }
                LSHFInnerPage.LSHInnerPageIterator iterator = innerPage.iterator();
                while (scanNext && iterator.hasNext()) {
                    LSHDto next = iterator.next();
                    if (next.getPid()== -1 || next.getPid() == pageId){
                        continue;
                    }
                    scanNext = _rangeScanEachLayer(next.getPid(), v, distance, layer, hf, heapFilePages, rids, notMatchingLimit);
                }
                SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
            }
            else if (basePage.getPageType() == LSHFLeafPage.pageType){
                LSHFLeafPage leafPage = new LSHFLeafPage(basePage);
                Iterator<LSHDto> iterator = leafPage.iterator();
                int leafRecordCount = 0;
                int selectRecordCount = 0;
                while(iterator.hasNext()){
                    leafRecordCount += 1;
                    LSHDto next = iterator.next();
                    int dist = v.distanceTo(next.getV());
                    if (dist <= distance){
                        String ridKey = next.getPid() + "_" + next.getSid();
                        if (!rids.contains(ridKey)) {
                            RID rid = hf.insertRecord(next.toLeafTuple().getTupleByteArray());
                            heapFilePages.add(rid.pageNo.pid);
                            rids.add(ridKey);
                        }
                        selectRecordCount += 1;
                    }
                }
                SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);

                int notMatchingCount = leafRecordCount - selectRecordCount;
                if (leafRecordCount == 0){
                    scanNext = false;
                } else if ((double) notMatchingCount/leafRecordCount * 100 > notMatchingLimit) {
                    scanNext = false;
                }
            }
        return scanNext;
    }

    public Sort rangeScan(Vector100Dtype v, int distance) throws Exception {
        LSHLayerMap layerMap = LSHLayerMap.getInstance();
        Iterator<LSHLayer> iter = layerMap.iterator();
        String tempFileName = v.toString()+distance+"range_temp.heap";
        Heapfile tempHf = new Heapfile(tempFileName);
        Set<Integer> heapFilePages = new HashSet<>();
        Set<String> rids = new HashSet<>();

        boolean scanNext = true;

        while (iter.hasNext() && scanNext) {
            LSHLayer layer = iter.next();
            layer.getLayerStartPage();
            scanNext = this._rangeScanEachLayer(layer.getLayerStartPage(), v, distance, layer, tempHf, heapFilePages, rids, 70.0);
        }
        FldSpec[] projlist = {
            new FldSpec(new RelSpec(RelSpec.outer), 1),
            new FldSpec(new RelSpec(RelSpec.outer), 2), 
            new FldSpec(new RelSpec(RelSpec.outer), 3)
            };
        AttrType[] types = {
            new AttrType(AttrType.attrVector100D), 
            new AttrType(AttrType.attrInteger), 
            new AttrType(AttrType.attrInteger)
            };
        FileScan fs = new FileScan(tempFileName, types, null, (short)3, (short)3, projlist, null);
        return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200, heapFilePages.size(), v, rids.size());
    }

}