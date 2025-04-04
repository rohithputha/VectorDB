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
            LSHHeaderPage headerPage = LSHHeaderPage.LSHHeaderPageFactory.createHeaderPages(h, l, fileName);
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

            LSHashFunctionsMap lshhash = LSHashFunctionsMap.getInstance(this.fileName);
            LSHLayerMap lshLayerMap = LSHLayerMap.getInstance(this.fileName);
            // System.out.println(lshhash);
            // System.out.println(lshLayerMap);
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
        this.h = this.headerPage.getHashFunctionsPerLayer();
        this.headerPage.getHashFunctions(this.fileName);
        LSHashFunctionsMap functionsMap = LSHashFunctionsMap.getInstance(this.fileName);
        this.headerPage.getLayers(h, this.fileName);
        LSHLayerMap layerMap = LSHLayerMap.getInstance(this.fileName);

        this.fileName = fileName;
    }

    private void _insertOnEachLayer(int startPageId, Vector100Dtype key, RID rid, int layerId) throws Exception {
//        LSHFInnerPage startPage = new LSHFInnerPage(new PageId(startPageId),this.fileName);
        LSHBasePage currentPage = new LSHBasePage(new PageId(startPageId),this.fileName);
        int prevPageId = -1;
        LSHFLeafPage leafPageFound = null;
        int prevHashInConsideration = -1;
        do {
            if(currentPage.getPageType() == LSHFInnerPage.pageType){
                LSHFInnerPage innerPage = new LSHFInnerPage(currentPage,this.fileName);
                prevHashInConsideration = innerPage.getHashFunctionInConsideration();
                int fPid = innerPage.getBucketByKey(key);
                if (fPid == -1){
                    leafPageFound = new LSHFLeafPage(this.fileName);
                    innerPage.insertBucketByKey(key, leafPageFound.getCurPage().pid);
                    SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), true);
                }
                else{
                    prevPageId = innerPage.getCurPage().pid;
                    currentPage = new LSHBasePage(new PageId(fPid),this.fileName);
                    SystemDefs.JavabaseBM.unpinPage(new PageId(prevPageId), false);
                }
            }
            else {
                LSHFLeafPage leafPage = new LSHFLeafPage(currentPage,this.fileName);
                if (prevHashInConsideration == this.h-1){
                    leafPageFound = leafPage;
                }
                else{
                    leafPageFound = new LSHFLeafPage(this.fileName);
                    LSHFInnerPage prevInnerPage = new LSHFInnerPage(new PageId(prevPageId),this.fileName);
                    LSHFInnerPage middleInnerPage = new LSHFInnerPage(layerId,prevHashInConsideration+1,this.fileName);
                    middleInnerPage.insertBucketByKey(leafPage.getFirstVector().getV(), leafPage.getCurPage().pid);
                    middleInnerPage.insertBucketByKey(key, leafPageFound.getCurPage().pid);
                    if (prevInnerPage.deleteBucketByPageId(leafPage.getCurPage().pid)){
                        prevInnerPage.insertBucketByKey(key, middleInnerPage.getCurPage().pid);
                    }
                    else {
                        throw new Exception("insert failed");
                    }
                    SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
                    SystemDefs.JavabaseBM.unpinPage(prevInnerPage.getCurPage(), true);
                    SystemDefs.JavabaseBM.unpinPage(middleInnerPage.getCurPage(), true);
                }
            }
        } while (leafPageFound == null);

        leafPageFound.insert(key, rid);
        SystemDefs.JavabaseBM.unpinPage(leafPageFound.getCurPage(), true);
    }

    @Override
    public void insert(Vector100Dtype key, RID rid) throws Exception {
        LSHLayerMap layerMap = LSHLayerMap.getInstance(this.fileName);
        Iterator<LSHLayer> iter = layerMap.iterator();
        while (iter.hasNext()) {
            LSHLayer layer = iter.next();
            this._insertOnEachLayer(layer.getLayerStartPage(), key, rid, layer.getLayerId());
        }

    }

    public void close()
            throws PageUnpinnedException,
            InvalidFrameNumberException,
            HashEntryNotFoundException,
            ReplacerException, PageNotFoundException, HashOperationException, BufMgrException, PagePinnedException, IOException {
        if (headerPage != null) {
//            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
//            SystemDefs.JavabaseBM.flushAllPages();
            headerPage = null;
        }
    }

    public List<LSHDto> fileScan(LSHBasePage currentPage) throws Exception {
        List<LSHDto> leafPageIds = new ArrayList<>();

        // Get the current hash function index
        //int hashFuncIndex = currentPage.getHashFunctionInConsideration();

        // If this is the last hash function, collect leaf page IDs
        if (currentPage.getPageType() == LSHFLeafPage.pageType) {
            // Iterate through all slots in the current page
            Iterator<LSHDto> lshLeafDtoIterator = (new LSHFLeafPage(currentPage, this.fileName)).iterator();
            while (lshLeafDtoIterator.hasNext()) {
                leafPageIds.add(lshLeafDtoIterator.next());
            }
            return leafPageIds;
        } else if (currentPage.getPageType() == LSHFInnerPage.pageType) {
            LSHFInnerPage currentInnerPage = new LSHFInnerPage(currentPage, this.fileName);
            for (short i = 2; i < currentInnerPage.getSlotCnt(); i++) {
                Tuple t = currentInnerPage.getRecord(new RID(currentInnerPage.getCurPage(), i));
                t.setHdr((short) 2,
                        new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                        null);

                // Get the page ID for the next level
                int nextPageId = t.getIntFld(2);
                // Recursively process the next inner page
                LSHBasePage nextPage = new LSHBasePage(new PageId(nextPageId), this.fileName);
                leafPageIds.addAll(fileScan(nextPage));
            }
            SystemDefs.JavabaseBM.unpinPage(currentInnerPage.getCurPage(), false);
        }

        // If not the last hash function, recursively process buckets
        return leafPageIds;
    }


    private int _nearestNeighboursEachLayer(int currentPageId, Vector100Dtype v, int k, LSHLayer layer, Heapfile hf, boolean considerAll, Set<Integer> heapFilePages, Set<String> rids) throws ConstructPageException, FieldNumberOutOfBoundException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, SpaceNotAvailableException, HFDiskMgrException, HFException, HFBufMgrException {
        if(k<0){
            return 0;
        }
        LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId), this.fileName);
        int a = 0;
        if(basePage.getPageType() == LSHFInnerPage.pageType){
            if (!considerAll) {
                LSHFInnerPage innerPage = new LSHFInnerPage(basePage, this.fileName);
                int pageId = innerPage.getBucketByKey(v);
                int hashInConsideration = innerPage.getHashInConsideration(v);
                if (pageId != INVALID_PAGE){
                    a = _nearestNeighboursEachLayer(pageId,v,k,layer, hf, false, heapFilePages, rids);
                    if (a < k) {
                        Iterator<List<LSHDto>> iterator = innerPage.expansionIterator(hashInConsideration);
                        while (iterator.hasNext() && a < k) {
                            List<LSHDto> next = iterator.next();

                            for (LSHDto dto : next) {
                                if (dto.getPid()== -1){
                                    continue;
                                }
                                if (dto.getPid() == pageId){
                                    continue;
                                }
                                a += _nearestNeighboursEachLayer(dto.getPid(), v, k - a, layer, hf, true, heapFilePages, rids);
                            }

                        }
                    }
                }
                else {
                    Iterator<List<LSHDto>> iterator = innerPage.expansionIterator(hashInConsideration);
                    while (iterator.hasNext() && a < k) {
                        List<LSHDto> next = iterator.next();

                        for (LSHDto dto : next) {
                            if (dto.getPid()== -1){
                                continue;
                            }
                            if (dto.getPid() == pageId){
                                continue;
                            }
                            a += _nearestNeighboursEachLayer(dto.getPid(), v, k - a, layer, hf, true, heapFilePages, rids);
                        }

                    }
                }
                SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
            }
            else{
                LSHFInnerPage innerPage = new LSHFInnerPage(basePage, this.fileName);
                int hashInConsideration = innerPage.getHashInConsideration(v);
                Iterator<List<LSHDto>> iterator = innerPage.expansionIterator(hashInConsideration);
                while (iterator.hasNext() && a < k) {
                    List<LSHDto> next = iterator.next();
                    for (LSHDto dto : next) {
                        if (dto.getPid()== -1){
                            continue;
                        }
                        a += _nearestNeighboursEachLayer(dto.getPid(), v, k - a, layer, hf, true, heapFilePages, rids);
                    }

                }
                SystemDefs.JavabaseBM.unpinPage(innerPage.getCurPage(), false);
            }

        }
        else if (basePage.getPageType() == LSHFLeafPage.pageType){
            LSHFLeafPage leafPage = new LSHFLeafPage(basePage, this.fileName);
            Iterator<LSHDto> iterator = leafPage.iterator();
            while(iterator.hasNext()){
                LSHDto next = iterator.next();
                if (!rids.contains(next.getPid()+"_"+next.getSid())) {
                    //long ed = next.getV().distanceTo(v);
                    //System.out.print("collected "+ed+" ");
                    // for (int i= 0;i<100;i++){
                    //     System.out.print(next.getV().get(i)+",");
                    // }
                    // System.out.println();

                    RID rid = hf.insertRecord(next.toLeafTuple().getTupleByteArray());
                    a = a + 1;
                    heapFilePages.add(rid.pageNo.pid);
                    rids.add(next.getPid()+ "_" + next.getSid());
                    // System.out.println(rid.pageNo + "_" + rid.slotNo);
                }
            }
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
        }
        return a;
    }

    public Sort nearestNeighbourScan(Vector100Dtype v, int k) throws Exception {
        LSHLayerMap layerMap = LSHLayerMap.getInstance(this.fileName);
        Iterator<LSHLayer> iter = layerMap.iterator();
        String tempFileName = "nn_temp";
        Random random = new Random();
        tempFileName = tempFileName+"_"+random.nextInt()+"_"+random.nextInt()+".heap";
//        Random ran  = new Random();
//        tempFileName = tempFileName + ran.nextInt(1000);
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
        // System.out.println("Total collected " + totalCollected);
        // System.out.println("Total heap file pages " + heapFilePages.size());
        return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200,1, v, k);
    }



    private boolean _rangeScanEachLayer(int currentPageId, Vector100Dtype v,
                                        int distance, LSHLayer layer, Heapfile hf, Set<Integer> heapFilePages,
                                        Set<String> rids, double notMatchingLimit)
            throws ConstructPageException, FieldNumberOutOfBoundException,
            InvalidSlotNumberException, InvalidTupleSizeException, IOException,
            InvalidTypeException, HashEntryNotFoundException, InvalidFrameNumberException,
            PageUnpinnedException, ReplacerException, SpaceNotAvailableException, HFException,
            HFBufMgrException, HFDiskMgrException {
        LSHBasePage basePage = new LSHBasePage(new PageId(currentPageId), this.fileName);
        boolean scanNext = true;
        if(basePage.getPageType() == LSHFInnerPage.pageType){
            LSHFInnerPage innerPage = new LSHFInnerPage(basePage, this.fileName);
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
            LSHFLeafPage leafPage = new LSHFLeafPage(basePage, this.fileName);
            Iterator<LSHDto> iterator = leafPage.iterator();
            int leafRecordCount = 0;
            int selectRecordCount = 0;
            while(iterator.hasNext()){
                leafRecordCount += 1;
                LSHDto next = iterator.next();
                long dist = v.distanceTo(next.getV());
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
        LSHLayerMap layerMap = LSHLayerMap.getInstance(this.fileName);
        Iterator<LSHLayer> iter = layerMap.iterator();
        Random rand = new Random();
        String tempFileName = v.toString()+rand.nextInt(1000)+"rt.heap";

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
        return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200, Math.max(heapFilePages.size(),1), v, rids.size());
    }
}