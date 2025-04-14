package LSHFIndex;

import btree.ConstructPageException;
import bufmgr.*;
import diskmgr.*;
import global.AttrType;
import global.PageId;
import global.SystemDefs;
import global.TupleOrder;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.*;
import java.util.Iterator;

public class LSHIndexBatchInsert {
    private String indexName;
    private int h;
    private int L;
    private PageId headerPageId;
    private HFPage headerPage;
    private Iterator<LSHDto> iterator;

    public LSHIndexBatchInsert(String indexName, int h, int L, Iterator<LSHDto> iterator) throws InvalidPageNumberException, IOException, FileIOException, DiskMgrException, ConstructPageException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, HashEntryNotFoundException, BufferPoolExceededException, PageNotReadException, FieldNumberOutOfBoundException, HashOperationException, BufMgrException, PagePinnedException, InvalidTupleSizeException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTypeException {
        this.indexName = indexName;
        this.iterator = iterator;

        PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(indexName);
        if (headerPageId == null) {
            LSHHeaderPage headerPage = LSHHeaderPage.LSHHeaderPageFactory.createHeaderPages(h, L, this.indexName);
            if (headerPage == null) {
                throw new ConstructPageException(null, "LSH header page could not be constructed. Index file could not be created.");
            }
            headerPageId = headerPage.getCurPage();
            SystemDefs.JavabaseDB.add_file_entry(indexName, headerPageId);

            PageId t = SystemDefs.JavabaseDB.get_file_entry(indexName);
            assert t != null;
            this.headerPageId = headerPageId;
            this.headerPage = headerPage;
            this.h = h;
            this.L = L;

            LSHashFunctionsMap lshhash = LSHashFunctionsMap.getInstance(this.indexName);
            LSHLayerMap lshLayerMap = LSHLayerMap.getInstance(this.indexName);
            System.out.println(lshhash);
        } else {
            this.headerPage = new LSHHeaderPage(headerPageId);
        }
    }


    private void addTuplesToHeapFile(Heapfile hf, LSHDto dto) throws FieldNumberOutOfBoundException, InvalidTupleSizeException, IOException, InvalidTypeException, SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException {
        hf.insertRecord(dto.toLeafTuple().getTupleByteArray());
    }
    private Sort getSortedByLayer(List<LSHDto> l, int layerId) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidSlotNumberException, InvalidTypeException, InvalidRelation, FileScanException, TupleUtilsException, SortException {
        LSHLayer layer = LSHLayerMap.getInstance(this.indexName).getLayerByLayerId(layerId);
        Heapfile hf = new Heapfile(layer+".heap");

        for(LSHDto dto : l){
            this.addTuplesToHeapFile(hf, dto);
        }
        FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
        AttrType[] types = {new AttrType(AttrType.attrVector100D), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
        FileScan fs = new FileScan(layer+".heap", types, null, (short)3, (short)3, projlist, null);
        return new Sort(types, (short)3, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200,(int)Math.ceil(hf.getRecCnt()/18));
    }

    public void insert(){

    }
}
