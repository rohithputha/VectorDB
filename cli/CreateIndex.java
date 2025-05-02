package cli;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.IntegerKey;
import btree.RealKey;
import btree.StringKey;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.HFPage;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import diskmgr.PCounter;

public class CreateIndex implements VectorDbCommand {
    private String commandName;
    private String RELNAME;
    private Integer COLUMN_ID;
    private Integer L;
    private Integer h;
    public static final String COMMAND = "createindex";
    public CreateIndex(String args[]){
        this.commandName = args[0];
        this.RELNAME = args[1];
        this.COLUMN_ID = Integer.parseInt(args[2]);
        this.L = Integer.parseInt(args[3]);
        this.h = Integer.parseInt(args[4]);
    }

    @Override
   public String getCommand() {
    return this.commandName;
   }

   @Override
   public void process(){

        long startTime = System.nanoTime();

        if(this.getEnvironment().getDb() != null){
            System.out.println("working in " + this.getEnvironment().getDb() );
            try{
                Heapfile dataHeapFile = new Heapfile(RELNAME);
                PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + RELNAME);
                HFPage page = new HFPage();
                SystemDefs.JavabaseBM.pinPage(pid, page, false);
                Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                m.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
                int input_fields_length = m.getIntFld(1);

                AttrType[] intattrarray = new AttrType[input_fields_length];

                for (int i = 0; i < input_fields_length; i++) {
                    //int typeInt = Integer.parseInt(typeStrs[i]);
                    intattrarray[i] = new AttrType(AttrType.attrInteger);
                }

                Tuple f = page.getRecord(new RID(page.getCurPage(), 1));
                f.setHdr((short) input_fields_length,
                        intattrarray,
                        null);

                AttrType[] schemaAttrTypes = new AttrType[input_fields_length];


                for (int i = 0; i < input_fields_length; i++) {
                    int fldtype = f.getIntFld(i + 1);
                    if (fldtype == 1) {
                        schemaAttrTypes[i] = new AttrType(AttrType.attrInteger);
                    } else if (fldtype == 2) {
                        schemaAttrTypes[i] = new AttrType(AttrType.attrReal);
                    } else if (fldtype == 3) {
                        schemaAttrTypes[i] = new AttrType(AttrType.attrString);
                    } else if (fldtype == 4) {
                        schemaAttrTypes[i] = new AttrType(AttrType.attrVector100D);
                    }
                }
                SystemDefs.JavabaseBM.unpinPage(pid, false);

                PageId pidhL = SystemDefs.JavabaseDB.get_file_entry("handL" + RELNAME);
                Tuple x = new Tuple();

                // x.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                //         null);
                x.setHdr((short) 3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                        null);
                x.setIntFld(1, h);
                x.setIntFld(2, L);
                x.setIntFld(3, COLUMN_ID);

                if(pidhL == null){
                    Page page_x = new Page();
                    PageId newPageId = SystemDefs.JavabaseBM.newPage(page_x, 1);
                    HFPage hfPage_x = new HFPage();
                    hfPage_x.init(newPageId, page_x);

                    hfPage_x.insertRecord(x.getTupleByteArray());
                    SystemDefs.JavabaseDB.add_file_entry("handL"+RELNAME, hfPage_x.getCurPage());
                    SystemDefs.JavabaseBM.unpinPage(hfPage_x.getCurPage(), true);
                }
                else{
                    HFPage hfPage_x = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pidhL, hfPage_x, false);
                    // hfPage_x.deleteRecord(new RID(new PageId(pidhL.pid), 0));
                    hfPage_x.insertRecord(x.getTupleByteArray());
                    SystemDefs.JavabaseBM.unpinPage(hfPage_x.getCurPage(), true);
                }

                // Initialize string sizes array (needed for tuple creation)
                short[] strSizes = new short[input_fields_length];
                // Assuming max string length of 100 for string attributes
                for (int i = 0; i < input_fields_length ; i++) {
                    if (schemaAttrTypes[i].attrType == AttrType.attrString) {
                        strSizes[i] = 100;
                    }
                }

                // if (schemaAttrTypes[COLUMN_ID].attrType == AttrType.attrVector100D) {
                if (schemaAttrTypes[COLUMN_ID - 1].attrType == AttrType.attrVector100D) {
                    // Create LSH index for vector
                    String indexName = RELNAME + "_" + COLUMN_ID + "_" + L + "_" + h;
                    LSHFIndex.LSHFIndexFile indexFile = new LSHFIndex.LSHFIndexFile(indexName, h, L);
                    
                    // Scan the heap file to build the index
                    Scan scan = dataHeapFile.openScan();
                    RID rid = new RID();
                    Tuple tuple = new Tuple();
                    tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                    
                    while ((tuple = scan.getNext(rid)) != null) {
                        if (tuple != null) {
                            tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                            // Vector100Dtype vector = tuple.get100DVectFld(COLUMN_ID + 1);
                            Vector100Dtype vector = tuple.get100DVectFld(COLUMN_ID);
                            // System.out.println(vector.get(0)+" "+rid.pageNo.pid+" "+rid.slotNo);
                            indexFile.insert(vector, rid);
                        }
                    }
                    
                    scan.closescan();
                    indexFile.close();
                    
                    // Store index metadata
                    //storeIndexMetadata(relName, columnId, "LSH", L, h, indexName);
                    
                    System.out.println("LSH index " + indexName + " created successfully.");
                }else{
                    System.out.println("Not Vector");
                    String indexName = RELNAME + "_" + COLUMN_ID;

                    // if(schemaAttrTypes[COLUMN_ID].attrType == AttrType.attrInteger){
                    if(schemaAttrTypes[COLUMN_ID - 1].attrType == AttrType.attrInteger){
                        BTreeFile btf = new BTreeFile(indexName, AttrType.attrInteger, 4, DeleteFashion.NAIVE_DELETE);
                        Scan scan = dataHeapFile.openScan();
                        RID rid = new RID();

                        Tuple tuple;
                        while ((tuple = scan.getNext(rid)) != null){
                            tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                            // Integer intValue = tuple.getIntFld(COLUMN_ID+1);
                            Integer intValue = tuple.getIntFld(COLUMN_ID);
                            IntegerKey key = new IntegerKey(intValue);
                            btf.insert(key, rid);
                        }
                        scan.closescan();
                        btf.close();
                    } 
                    // else if (schemaAttrTypes[COLUMN_ID].attrType == AttrType.attrReal){
                    else if (schemaAttrTypes[COLUMN_ID - 1].attrType == AttrType.attrReal){
                        BTreeFile btf = new BTreeFile(indexName, AttrType.attrReal, 8, DeleteFashion.NAIVE_DELETE);
                        Scan scan = dataHeapFile.openScan();
                        RID rid = new RID();

                        Tuple tuple;
                        while ((tuple = scan.getNext(rid)) != null){
                            tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                            // Float realValue = tuple.getFloFld(COLUMN_ID+1);
                            Float realValue = tuple.getFloFld(COLUMN_ID);
                            RealKey key = new RealKey(realValue);
                            btf.insert(key, rid);
                        }
                        scan.closescan();
                        btf.close();
                    } 
                    // else if (schemaAttrTypes[COLUMN_ID].attrType == AttrType.attrString){
                    else if (schemaAttrTypes[COLUMN_ID - 1].attrType == AttrType.attrString){
                        BTreeFile btf = new BTreeFile(indexName, AttrType.attrString, 100, DeleteFashion.NAIVE_DELETE);
                        Scan scan = dataHeapFile.openScan();
                        RID rid = new RID();

                        Tuple tuple;
                        while ((tuple = scan.getNext(rid)) != null){
                            tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                            // String stringValue = tuple.getStrFld(COLUMN_ID+1);
                            String stringValue = tuple.getStrFld(COLUMN_ID);
                            StringKey key = new StringKey(stringValue);
                            btf.insert(key, rid);
                        }
                        scan.closescan();
                        btf.close();
                    }
                    System.out.println("Btree index with name - " + indexName + " created successfully.");
                    
                } 

                SystemDefs.JavabaseBM.flushAllPages();


            } catch (Exception e) {

                printer("Error creating index "+e.getMessage());
                return;
            }
        }
        else{
            System.out.println("Database not open");
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");

        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());
        
   }
}
