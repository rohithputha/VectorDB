package cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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

public class BatchCreateCommand implements VectorDbCommand {
   private String commandName; 
   private String DATAFILENAME;
   private String RELNAME; 
   public static final String COMMAND = "batchcreate";
   public BatchCreateCommand(String args[]){
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
        //SystemDefs systemDefs = null;

        long startTime = System.nanoTime();

        if(this.getEnvironment().getDb() != null){
            System.out.println("working in " + this.getEnvironment().getDb() );
            try {
                BufferedReader br = new BufferedReader(new FileReader(DATAFILENAME));
                int numAttributes = Integer.parseInt(br.readLine().trim());

                String[] typeStrs = br.readLine().trim().split("\\s+");
                AttrType[] attrTypes = new AttrType[numAttributes];
                List<Integer> vectorFields = new ArrayList<>();

                AttrType[] intattrarray = new AttrType[numAttributes];
                for(int i=0; i<numAttributes; i++){
                    //int typeInt = Integer.parseInt(typeStrs[i]);
                    intattrarray[i] = new AttrType(AttrType.attrInteger);
                }

                PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + RELNAME);

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
                    SystemDefs.JavabaseDB.add_file_entry("metadata"+RELNAME, hfPage.getCurPage());
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

                Heapfile dataHeapFile = new Heapfile(RELNAME);

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

                SystemDefs.JavabaseBM.flushAllPages();





            } catch (Exception e) {
                
                printer("Error in batch  insert "+e.getMessage());
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
