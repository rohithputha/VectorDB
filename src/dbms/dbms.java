package dbms;

import java.nio.ByteBuffer;
import java.util.*;

import diskmgr.PCounter;
import diskmgr.Page;

import java.io.*;

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

public class dbms {
    private String currentDatabase;
    //private SystemDefs sysdef;

    // File to persist the database state
    private static final String DB_STATE_FILE = "dbstate.txt";

    public dbms() {
        // Load the database state from the file at startup
        // currentDatabase = loadDatabaseState();
        // if (currentDatabase!=null){
        //     SystemDefs sysdef = new SystemDefs(currentDatabase, 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        // }
        this.currentDatabase=currentDatabase;
        
    }

    public void closeDataBase() throws Exception {
        if (currentDatabase == null) {
            System.out.println("No database is open!");
            return;
        }
        System.out.println("Closing database: " + currentDatabase);
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println(currentDatabase + " closed successfully");
        currentDatabase = null;
        saveDatabaseState(null);  // Clear saved state
    }

    public void openDataBase(String dbname) throws Exception {
        if (currentDatabase != null) {
            closeDataBase();
        }
        File f = new File("./" + dbname);

        boolean dbcheck = f.exists() && !f.isDirectory();

        if (dbcheck) {
            System.out.println("Opening an existing database with name " + dbname);
            SystemDefs sysdef = new SystemDefs(dbname, 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        } else {
            System.out.println("Creating new database with name " + dbname);
            SystemDefs sysdef = new SystemDefs(dbname, GlobalConst.MINIBASE_DB_SIZE, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        }
        currentDatabase = dbname;
        saveDatabaseState(currentDatabase);  // Save the state to the file
        System.out.println("Current database is now set to: " + currentDatabase);
    }

    // Save the current database state to a file
    private void saveDatabaseState(String dbName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DB_STATE_FILE))) {
            if (dbName != null) {
                writer.write(dbName);
            }
        } catch (IOException e) {
            System.out.println("Failed to save database state: " + e.getMessage());
        }
    }

    // Load the current database state from the file
    private String loadDatabaseState() {
        try (BufferedReader reader = new BufferedReader(new FileReader(DB_STATE_FILE))) {
            return reader.readLine();  // Read the first line (stored database name)
        } catch (IOException e) {
            return null;  // No database state found
        }
    }

    public void batchcreate(String dataFileName, String dbName) throws Exception{
        SystemDefs sysdef = new SystemDefs(dbName, GlobalConst.MINIBASE_DB_SIZE, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");

        BufferedReader br = new BufferedReader(new FileReader(dataFileName));

        int numAttributes = Integer.parseInt(br.readLine().trim());

        String[] typeStrs = br.readLine().trim().split("\\s+");
        AttrType[] attrTypes = new AttrType[numAttributes];
        List<Integer> vectorFields = new ArrayList<>();

        AttrType[] intattrarray = new AttrType[numAttributes];
        for(int i=0; i<numAttributes; i++){
            //int typeInt = Integer.parseInt(typeStrs[i]);
            intattrarray[i] = new AttrType(AttrType.attrInteger);
        }

        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + dbName);
        
        Tuple m = new Tuple();
        m.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)},
                null);
        m.setIntFld(1, numAttributes);

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
            SystemDefs.JavabaseDB.add_file_entry("metadata"+dbName, hfPage.getCurPage());
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

        Heapfile dataHeapFile = new Heapfile(dbName);

        // Initialize string sizes array (needed for tuple creation)
        short[] strSizes = new short[numAttributes];
        // Assuming max string length of 100 for string attributes
        for (int i = 0; i < numAttributes; i++) {
            if (attrTypes[i].attrType == AttrType.attrString) {
                strSizes[i] = 100;
            }
        }

         // Start reading data tuples
         String line;
         int tupleCount = 0;
 
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
            dataHeapFile.insertRecord(tuple.getTupleByteArray());
            tupleCount++;

        }

        br.close();

         System.out.println("Batch creation completed.");
        System.out.println("Total tuples inserted: " + tupleCount);
        
        // End the database session
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

    }

    public void createIndex(String dbName, Integer COLUMN_ID, Integer h, Integer L) throws Exception{
        SystemDefs sysdef = new SystemDefs(dbName, 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        Heapfile dataHeapFile = new Heapfile(dbName);
        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + dbName);
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

        PageId pidhL = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
        Tuple x = new Tuple();

        x.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                null);
        x.setIntFld(1, h);
        x.setIntFld(2, L);

        if(pidhL == null){
            Page page_x = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(page_x, 1);
            HFPage hfPage_x = new HFPage();
            hfPage_x.init(newPageId, page_x);

            hfPage_x.insertRecord(x.getTupleByteArray());
            SystemDefs.JavabaseDB.add_file_entry("handL"+dbName, hfPage_x.getCurPage());
            SystemDefs.JavabaseBM.unpinPage(hfPage_x.getCurPage(), true);
        }
        else{
            HFPage hfPage_x = new HFPage();
            SystemDefs.JavabaseBM.pinPage(pidhL, hfPage_x, false);
            hfPage_x.deleteRecord(new RID(new PageId(pidhL.pid), 0));
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

        if (schemaAttrTypes[COLUMN_ID].attrType == AttrType.attrVector100D) {
                // Create LSH index for vector
                String indexName = dbName + "_" + COLUMN_ID + "_" + L + "_" + h;
                LSHFIndex.LSHFIndexFile indexFile = new LSHFIndex.LSHFIndexFile(indexName, h, L);
                
                // Scan the heap file to build the index
                Scan scan = dataHeapFile.openScan();
                RID rid = new RID();
                Tuple tuple = new Tuple();
                tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                
                while ((tuple = scan.getNext(rid)) != null) {
                    if (tuple != null) {
                        tuple.setHdr((short) input_fields_length, schemaAttrTypes, strSizes);
                        Vector100Dtype vector = tuple.get100DVectFld(COLUMN_ID + 1);
                        System.out.println(vector.get(0)+" "+rid.pageNo.pid+" "+rid.slotNo);
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
            } 
    }

    public static void main(String[] args) throws Exception {
        dbms dbInstance = new dbms();  // Create dbms instance to manage database

        int len_arg = args.length;

        if (len_arg == 1 && args[0].equals("close_database")) {
            System.out.println("Implement closing database");
            dbInstance.closeDataBase();
        } else if (len_arg == 2 && args[0].equals("open_database")) {
            String dbname = args[1];
            System.out.println("Implement open database " + dbname);
            dbInstance.openDataBase(dbname);
        } else if (len_arg == 3) {
            String action = args[0];
            String filename = args[1];
            String relname = args[2];
            if (action.equals("batchcreate")) {
                System.out.println("batchcreate");
                dbInstance.batchcreate(filename, relname);
            } else if (action.equals("batchinsert")) {
                System.out.println("batchinsert");
            } else if (action.equals("batchdelete")) {
                System.out.println("batchdelete");
            } else {
                System.out.println("wrong input");
            }
        } else if (len_arg == 5 && args[0].equals("createindex")) {
            System.out.println("Implement index creation");
            String dbname = args[1];
            Integer col_id = Integer.parseInt(args[2]);
            Integer L = Integer.parseInt(args[3]);
            Integer h = Integer.parseInt(args[4]); 
            dbInstance.createIndex(dbname, col_id, h, L);
        } else {
            System.out.println("wrong input");
        }
    }
}
