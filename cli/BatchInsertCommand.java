package cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.HFPage;
import heap.Heapfile;
import heap.Tuple;
import diskmgr.PCounter;


public class BatchInsertCommand implements VectorDbCommand {
   private String commandName; 
   private String DATAFILENAME;
   private String RELNAME; 
   public static final String COMMAND = "batchinsert";
   public BatchInsertCommand(String args[]){
    this.commandName = args[0];
    this.DATAFILENAME = args[1];
    this.RELNAME = args[2];
   }

   @Override
   public String getCommand() {
    return this.commandName;
   }

   @Override
   public void process() {

        long startTime = System.nanoTime();

        if(this.getEnvironment().getDb() != null){
            System.out.println("working in " + this.getEnvironment().getDb() );
            try {
                PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + RELNAME);
                if (pid == null){
                    throw new Exception("Relation " + RELNAME + " does not exist in database");
                } else {

                    System.out.println("Executing batchinsert on: " + RELNAME);

                    // Extract schema metadata from database
                    HFPage page = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pid, page, false);
                    Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                    m.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)},null);
                    int dbNumAttributes = m.getIntFld(1);
                    // System.out.println("DB Schema Number of Atrributes: " + dbNumAttributes);

                    AttrType[] dbintattrarray = new AttrType[dbNumAttributes];
                    for (int i = 0; i < dbNumAttributes; i++) {
                        dbintattrarray[i] = new AttrType(AttrType.attrInteger);
                    }

                    Tuple f = page.getRecord(new RID(page.getCurPage(), 1));
                    f.setHdr((short) dbNumAttributes, dbintattrarray, null);
                    AttrType[] dbAttrTypes = new AttrType[dbNumAttributes];

                    for (int i = 0; i < dbNumAttributes; i++) {
                        int fldtype = f.getIntFld(i + 1);
                        // System.out.println("DB Schema Attribute Type: " + fldtype);
                        if (fldtype == 1) {
                            dbAttrTypes[i] = new AttrType(AttrType.attrInteger);
                        } else if (fldtype == 2) {
                            dbAttrTypes[i] = new AttrType(AttrType.attrReal);
                        } else if (fldtype == 3) {
                            dbAttrTypes[i] = new AttrType(AttrType.attrString);
                        } else if (fldtype == 4) {
                            dbAttrTypes[i] = new AttrType(AttrType.attrVector100D);
                        } else {
                            throw new Exception("Unknown attribute type in relation metadata " + fldtype);
                        }
                    }
                    SystemDefs.JavabaseBM.unpinPage(pid, false);

                    // Extract schema metadata from data file
                    BufferedReader br = new BufferedReader(new FileReader(DATAFILENAME));
                    int numAttributes = Integer.parseInt(br.readLine().trim());
                    // System.out.println("File Schema Number of Atrributes: " + dbNumAttributes);

                    String[] typeStrs = br.readLine().trim().split("\\s+");
                    AttrType[] attrTypes = new AttrType[numAttributes];
                    List<Integer> vectorFields = new ArrayList<>();

                    AttrType[] fileintattrarray = new AttrType[numAttributes];
                    for(int i=0; i<numAttributes; i++){
                        fileintattrarray[i] = new AttrType(AttrType.attrInteger);
                    }

                    for (int i = 0; i < numAttributes; i++) {
                        int typeInt = Integer.parseInt(typeStrs[i]);
                        // System.out.println("File Schema Attribute Type: " + typeInt);
                        switch(typeInt) {
                            case 1:
                                attrTypes[i] = new AttrType(AttrType.attrInteger);
                                break;
                            case 2:
                                attrTypes[i] = new AttrType(AttrType.attrReal);
                                break;
                            case 3:
                                attrTypes[i] = new AttrType(AttrType.attrString);
                                break;
                            case 4:
                                attrTypes[i] = new AttrType(AttrType.attrVector100D);
                                vectorFields.add(i);
                                break;
                            default:
                                throw new Exception("Unknown attribute type: " + typeInt);
                        }
                    }

                    // Validate attribute count
                    if (dbNumAttributes != numAttributes) {
                        throw new Exception("Attribute count mismatch");
                    }

                    //validate attribute type
                    for (int i = 0; i < dbNumAttributes; i++) {
                        if (attrTypes[i].attrType != dbAttrTypes[i].attrType) {
                            throw new Exception("Attribute type mismatch at position " + i);
                        }
                    }

                    Heapfile dataHeapFile = new Heapfile(RELNAME);
                    int beforeInsertRecCnt = dataHeapFile.getRecCnt();

                    // Initialize string sizes array (needed for tuple creation)
                    short[] strSizes = new short[numAttributes];
                    for (int i = 0; i < numAttributes; i++) {
                        if (attrTypes[i].attrType == AttrType.attrString) {
                            strSizes[i] = 100;
                        }
                    }

                    // Map to store RIDs of inserted records - Need in case indexes exist for the relation 
                    Map<Integer, List<RID>> tupleRIDs = new HashMap<>();
                    for (Integer field : vectorFields) {
                        tupleRIDs.put(field, new ArrayList<>());
                    }

                    // Process records in data file
                    String line;
                    int tupleCount = 0;
            

                    System.out.println("Inserting records to the data file");
                    line = br.readLine();
                    while (line != null && !line.trim().isEmpty()) {
                        Tuple tuple = new Tuple();

                        // Initialize the tuple
                        tuple.setHdr((short) numAttributes, attrTypes, strSizes);

                        // Read attribute values for the tuple
                        for (int i = 0; i < numAttributes; i++) {
                            switch (attrTypes[i].attrType) {
                                case AttrType.attrInteger:
                                    int intVal = Integer.parseInt(line.trim());
                                    tuple.setIntFld(i+1, intVal);
                                    break;
                                case AttrType.attrReal:
                                    //System.out.println("This is a real value");
                                    float floatVal = Float.parseFloat(line.trim());
                                    //System.out.println(floatVal);
                                    tuple.setFloFld(i+1, floatVal);
                                    //System.out.println("Done with real");
                                    break;
                                case AttrType.attrString:
                                    String strVal = line.trim();
                                    tuple.setStrFld(i+1, strVal);
                                    break;
                                case AttrType.attrVector100D:
                                    //System.out.println("This is a vector value");
                                    String[] vectorVals = line.trim().split("\\s+");
                                    if (vectorVals.length != 100) {
                                        throw new Exception("Vector must have 100 dimensions, found: " + vectorVals.length);
                                    }
                                    // Create an array to hold the 100 integer values
                                    short[] dimensions = new short[100];
                                    for (int j = 0; j < 100; j++) {
                                        Double decimalValue = Double.parseDouble(vectorVals[j]);
                                        dimensions[j] = decimalValue.shortValue();
                                    }
                                    // Assuming Vector100Dtype has a constructor that takes an int array
                                    Vector100Dtype vector = new Vector100Dtype(dimensions);

                                    tuple.set100DVectFld(i+1, vector);
                                    break;
                            }
                            line = br.readLine();
                        }
                        RID rid = dataHeapFile.insertRecord(tuple.getTupleByteArray());

                        // Store the RID for each vector field - Need in case indexes exist for the relation
                        for (Integer field : vectorFields) {
                            tupleRIDs.get(field).add(new RID(rid.pageNo, rid.slotNo));
                        }

                        tupleCount++;
                    }
                    br.close();

                    long dataFileInsertEndTime = System.nanoTime();
                    long dataFileInsertDuration = dataFileInsertEndTime - startTime;
                    System.out.println("Execution Time: " + (dataFileInsertDuration / 1_000_000) + " milliseconds");
                    System.out.println("Read Counter Value: " + PCounter.getReads());
                    System.out.println("Write Counter Value: " + PCounter.getWrites());

                    // check existance of index on relation and add records on indexes
                    PageId pidhL = SystemDefs.JavabaseDB.get_file_entry("handL" + RELNAME);
                    if (pidhL == null) {
                        System.out.println("No index defined for the relation");
                    } else {

                        long indexFileInsertStartTime = System.nanoTime();
                        long indexFileInsertEndTime = System.nanoTime();
                        long indexFileInsertDuration = dataFileInsertEndTime - startTime;

                        HFPage hfPage_x = new HFPage();
                        SystemDefs.JavabaseBM.pinPage(pidhL, hfPage_x, false);

                        RID currentRid = hfPage_x.firstRecord();

                        while (currentRid != null) {

                            indexFileInsertStartTime = System.nanoTime();

                            Tuple idxMetaData = hfPage_x.getRecord(currentRid);
                            idxMetaData.setHdr((short) 3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},null);

                            int h = idxMetaData.getIntFld(1);
                            int L = idxMetaData.getIntFld(2);
                            int colId = idxMetaData.getIntFld(3);

                            // System.out.println("H: " + h);
                            // System.out.println("L: " + L);
                            // System.out.println("ColId: " + colId);

                            String indexName = RELNAME + "_" + colId + "_" + L + "_" + h;
                            
                            // LSHFIndex.LSHFIndexFile indexFile = new LSHFIndex.LSHFIndexFile(indexName, h, L);
                            LSHFIndex.LSHFIndexFile indexFile = new LSHFIndex.LSHFIndexFile(indexName);

                            System.out.println("Updating index: " + indexName);

                            for (RID rid : tupleRIDs.get(colId)) {
                                Tuple tuple_x = new Tuple();
                                tuple_x.setHdr((short) numAttributes, attrTypes, strSizes);
                                tuple_x = dataHeapFile.getRecord(rid);
                                tuple_x.setHdr((short) numAttributes, attrTypes, strSizes);
                                Vector100Dtype vectorKey = tuple_x.get100DVectFld(colId + 1);
                                // System.out.println(vectorKey.get(0)+" "+rid);
                                indexFile.insert(vectorKey, rid);
                            }
                            indexFile.close();
                            System.out.println("Index update completed");
                            
                            indexFileInsertEndTime = System.nanoTime();
                            indexFileInsertDuration = indexFileInsertEndTime - indexFileInsertStartTime;
                            System.out.println("Execution Time: " + (indexFileInsertDuration / 1_000_000) + " milliseconds");
                            System.out.println("Read Counter Value: " + PCounter.getReads());
                            System.out.println("Write Counter Value: " + PCounter.getWrites());
                            

                            currentRid = hfPage_x.nextRecord(currentRid);
                        }
                        SystemDefs.JavabaseBM.unpinPage(pidhL, false);
                    }


                    int afterInsertRecCnt = dataHeapFile.getRecCnt();

                    System.out.println("Before execution record count in " + RELNAME + " " + beforeInsertRecCnt);
                    System.out.println("Total tuples inserted: " + tupleCount);
                    System.out.println("After execution record count in " + RELNAME + " " + afterInsertRecCnt);
                    
                    System.out.println("Batch insert completed.");

                    SystemDefs.JavabaseBM.flushAllPages();
                }
            } catch (Exception e) {
                printer("Error in batch insert "+e.getMessage());
                return;
            }
        }
        else {
            System.out.println("Database not open");
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");
        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());
   }



}
