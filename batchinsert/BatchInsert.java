package batchinsert;

import global.*;
import heap.*;
import index.*;
import iterator.*;
import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import heap.*;
import diskmgr.Page;
import diskmgr.*;
import LSHFIndex.*;

public class BatchInsert {
    private int h;              // Number of hash functions per layer
    private int L;              // Number of layers
    private String dataFileName; // Input data file name
    private String dbName;      // Database name

    // Statistics counters
    // private int pagesRead = 0;
    // private int pagesWritten = 0;

    /**
     * Constructor
     */
    public BatchInsert(int h, int L, String dataFileName, String dbName) {
        this.h = h;
        this.L = L;
        this.dataFileName = dataFileName;
        this.dbName = dbName;
    }

    /**
     * Main method to process the batch insertion
     */
    public void process() throws Exception {
        // Create/open the database
        SystemDefs sysdef = new SystemDefs(dbName, GlobalConst.MINIBASE_DB_SIZE, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        // sysdef.h = h;
        // sysdef.L = L;

        //ByteBuffer buffer = ByteBuffer.allocate(8);

        // Tuple m = hfPage.getRecord(new RID(hfPage.getCurPage(), 0));
        // m.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
        //                 null);
        // m.setIntFld(1, h);
        // m.setIntFld(2, L);


        // Read data file format
        BufferedReader br = new BufferedReader(new FileReader(dataFileName));

        // Read number of attributes
        int numAttributes = Integer.parseInt(br.readLine().trim());
        System.out.println(numAttributes);

        // Read attribute types
        String[] typeStrs = br.readLine().trim().split("\\s+");
        System.out.println("Read attributes");
        AttrType[] attrTypes = new AttrType[numAttributes];
        List<Integer> vectorFields = new ArrayList<>();

        AttrType[] intattrarray = new AttrType[numAttributes];

        for(int i=0; i<numAttributes; i++){
            //int typeInt = Integer.parseInt(typeStrs[i]);
            intattrarray[i] = new AttrType(AttrType.attrInteger);
        }

        PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
        Tuple m = new Tuple();
        m.setHdr((short) 3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                null);
        m.setIntFld(1, h);
        m.setIntFld(2, L);
        m.setIntFld(3, numAttributes);

        Tuple k = new Tuple();
        k.setHdr((short) numAttributes, intattrarray,
                null);

        for(int i=0; i<numAttributes; i++){
            k.setIntFld(i+1, Integer.parseInt(typeStrs[i]));
        }

        if(pid == null){
            Page page = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(page, 1);
            HFPage hfPage = new HFPage();
            hfPage.init(newPageId, page);

            hfPage.insertRecord(m.getTupleByteArray());
            hfPage.insertRecord(k.getTupleByteArray());
            SystemDefs.JavabaseDB.add_file_entry("handL"+dbName, hfPage.getCurPage());
            SystemDefs.JavabaseBM.unpinPage(hfPage.getCurPage(), true);
        }
        else{
            HFPage hfPage = new HFPage();
            SystemDefs.JavabaseBM.pinPage(pid, hfPage, false);
            hfPage.deleteRecord(new RID(new PageId(pid.pid), 0));
            hfPage.insertRecord(m.getTupleByteArray());
            hfPage.deleteRecord(new RID(new PageId(pid.pid), 1));
            hfPage.insertRecord(k.getTupleByteArray());
            SystemDefs.JavabaseBM.unpinPage(hfPage.getCurPage(), true);

        }




        for (int i = 0; i < numAttributes; i++) {
            int typeInt = Integer.parseInt(typeStrs[i]);
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


        for(AttrType i: attrTypes){
            System.out.println(i);
        }

        // Create a heap file for the data
        Heapfile dataHeapFile = new Heapfile(dbName);

        // Initialize string sizes array (needed for tuple creation)
        short[] strSizes = new short[numAttributes];
        // Assuming max string length of 100 for string attributes
        for (int i = 0; i < numAttributes; i++) {
            if (attrTypes[i].attrType == AttrType.attrString) {
                strSizes[i] = 100;
            }
        }

        // Map to store RIDs of inserted records - we'll need these for creating indexes
        Map<Integer, List<RID>> tupleRIDs = new HashMap<>();
        for (Integer field : vectorFields) {
            tupleRIDs.put(field, new ArrayList<>());
        }

        // Start reading data tuples
        String line;
        int tupleCount = 0;

        //System.out.println(br.readLine().trim());
        //System.out.println(br.readLine().trim());
        line = br.readLine();
        while (line != null && !line.trim().isEmpty()) {
            Tuple tuple = new Tuple();
            //System.out.println(line);

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

            // Insert the tuple into the heap file and store the RID
            RID rid = dataHeapFile.insertRecord(tuple.getTupleByteArray());

            // Store the RID for each vector field
            for (Integer field : vectorFields) {
                tupleRIDs.get(field).add(new RID(rid.pageNo, rid.slotNo));
            }

            tupleCount++;
            // pagesWritten++; // Estimate page writes

        }
        //System.out.println(tupleCount);
        //System.out.println(pagesWritten);

        br.close();

        // Create LSH-forest index for each vector attribute
        for (Integer vectorField : vectorFields) {
            String indexName = dbName + vectorField + "_" + h + "_" + L;
            System.out.println(indexName);

            // Create the LSH-forest index file
            LSHFIndex.LSHFIndexFile indexFile = new LSHFIndex.LSHFIndexFile(indexName, h, L);

            // Since we can't get RIDs directly from FileScan, we'll use a scan to read tuples
            // in order and pair them with our stored RIDs
            Scan scan = dataHeapFile.openScan();
            RID scanRid = new RID();

            // For each stored RID for this vector field
            for (RID rid : tupleRIDs.get(vectorField)) {
                // Directly retrieve the tuple using the stored RID
                Tuple tuple = new Tuple();
                tuple.setHdr((short) numAttributes, attrTypes, strSizes);
                tuple = dataHeapFile.getRecord(rid);
                tuple.setHdr((short) numAttributes, attrTypes, strSizes);

                // Get the vector field as key
                Vector100Dtype vectorKey = tuple.get100DVectFld(vectorField + 1);
                System.out.println(vectorKey.get(0)+" "+rid);

                // Create a key object
                //Vector100DKey key = new Vector100DKey(vectorKey);

                // Insert the key-RID pair into the index
                indexFile.insert(vectorKey, rid);

                // pagesRead++;    // Estimate page reads
                // pagesWritten++; // Estimate page writes
            }

            // Close the scan
            scan.closescan();
            System.out.println("Closed the file scan");

            // Close the index
            indexFile.close();
        }

        // Print statistics
        //System.out.println("Slot count" + hfPage.getSlotCnt());

        System.out.println("Batch insertion completed.");
        System.out.println("Total tuples inserted: " + tupleCount);
        // System.out.println("Pages read (estimate): " + pagesRead);
        // System.out.println("Pages written (estimate): " + pagesWritten);
        System.out.println("Value of read counter: " + PCounter.rcounter);
        System.out.println("Value of write counter: " + PCounter.wcounter);

        // End the database session
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }

    /**
     * Main method to run the batch insert program
     */
    public static void main(String[] args) {

        long startTime = System.nanoTime();

        if (args.length != 4) {
            System.out.println("Usage: batchinsert h L DATAFILENAME DBNAME");
            System.exit(1);
        }

        try {
            int h = Integer.parseInt(args[0]);
            int L = Integer.parseInt(args[1]);
            String dataFileName = args[2];
            String dbName = args[3];

            BatchInsert batchInsert = new BatchInsert(h, L, dataFileName, dbName);
            batchInsert.process();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        double durationInMilliseconds = (double) duration / 1_000_000.0;
        System.out.println("Time taken for the step: " + durationInMilliseconds + " milliseconds");
    }
}