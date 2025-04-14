package iterator;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;

import btree.BTreeFile;
import btree.IntegerKey;
import diskmgr.*;
import bufmgr.*;

/**
 * Test class for Index Nested Loop Join
 */
public class INLJoinsTest {
    private static final int NUM_PAGES = 100; // Number of buffer pages to use
    
    /**
     * Create sample data for testing
     */
    private static void createSampleData(String outerFileName, String innerFileName) 
        throws Exception {
        // Create the outer relation file
        Heapfile outerHeapfile = new Heapfile(outerFileName);
        
        // Create the inner relation file
        Heapfile innerHeapfile = new Heapfile(innerFileName);
        
        // Define attribute types for outer relation (R)
        AttrType[] outerAttrTypes = new AttrType[3];
        outerAttrTypes[0] = new AttrType(AttrType.attrInteger); // ID
        outerAttrTypes[1] = new AttrType(AttrType.attrString);  // Name
        outerAttrTypes[2] = new AttrType(AttrType.attrInteger); // Age
        
        // Define attribute types for inner relation (S)
        AttrType[] innerAttrTypes = new AttrType[3];
        innerAttrTypes[0] = new AttrType(AttrType.attrInteger); // ID
        innerAttrTypes[1] = new AttrType(AttrType.attrString);  // Department
        innerAttrTypes[2] = new AttrType(AttrType.attrInteger); // Salary
        
        // Set up string sizes
        short[] outerStrSizes = new short[1];
        outerStrSizes[0] = 20; // Max size for the Name attribute
        
        short[] innerStrSizes = new short[1];
        innerStrSizes[0] = 20; // Max size for the Department attribute
        
        // Insert records into outer relation
        for (int i = 1; i <= 10; i++) {
            Tuple outerTuple = new Tuple();
            outerTuple.setHdr((short) 3, outerAttrTypes, outerStrSizes);
            
            outerTuple.setIntFld(1, i);                           // ID
            outerTuple.setStrFld(2, "Person" + i);                // Name
            outerTuple.setIntFld(3, 20 + i);                      // Age
            
            outerHeapfile.insertRecord(outerTuple.getTupleByteArray());
        }
        
        // Insert records into inner relation
        for (int i = 5; i <= 15; i++) {
            Tuple innerTuple = new Tuple();
            innerTuple.setHdr((short) 3, innerAttrTypes, innerStrSizes);
            
            innerTuple.setIntFld(1, i);                           // ID
            innerTuple.setStrFld(2, "Dept" + (i % 3 + 1));        // Department
            innerTuple.setIntFld(3, 1000 * i);                    // Salary
            
            innerHeapfile.insertRecord(innerTuple.getTupleByteArray());
        }
        
        System.out.println("Sample data created successfully.");
    }
    
    /**
     * Create B-Tree index on the inner relation
     */
    private static void createBTreeIndex(String innerFileName, String indexFileName) 
        throws Exception {
        // Define attribute types for inner relation
        AttrType[] innerAttrTypes = new AttrType[3];
        innerAttrTypes[0] = new AttrType(AttrType.attrInteger); // ID
        innerAttrTypes[1] = new AttrType(AttrType.attrString);  // Department
        innerAttrTypes[2] = new AttrType(AttrType.attrInteger); // Salary
        
        short[] innerStrSizes = new short[1];
        innerStrSizes[0] = 20; // Max size for the Department attribute
        
        // Create BTree index on ID field (field 1)
        BTreeFile btf = new BTreeFile(indexFileName, AttrType.attrInteger, 4, 1);
        
        // Scan the inner heapfile and create index entries
        Heapfile hf = new Heapfile(innerFileName);
        Scan scan = hf.openScan();
        
        RID rid = new RID();
        Tuple tuple = new Tuple();
        tuple.setHdr((short) 3, innerAttrTypes, innerStrSizes);
        
        while ((tuple = scan.getNext(rid)) != null) {
            tuple.setHdr((short) 3, innerAttrTypes, innerStrSizes);
            
            // Extract the key (ID field)
            int key = tuple.getIntFld(1);
            
            // Insert into the index
            btf.insert(new IntegerKey(key), rid);
        }
        
        scan.closescan();
        btf.close();
        
        System.out.println("BTree index created successfully.");
    }
    
    /**
     * Run the index nested loop join
     */
    private static void runINLJoin(String outerFileName, String innerFileName, String indexFileName) 
        throws Exception {
        // Define attribute types for outer relation (R)
        AttrType[] outerAttrTypes = new AttrType[3];
        outerAttrTypes[0] = new AttrType(AttrType.attrInteger); // ID
        outerAttrTypes[1] = new AttrType(AttrType.attrString);  // Name
        outerAttrTypes[2] = new AttrType(AttrType.attrInteger); // Age
        
        // Define attribute types for inner relation (S)
        AttrType[] innerAttrTypes = new AttrType[3];
        innerAttrTypes[0] = new AttrType(AttrType.attrInteger); // ID
        innerAttrTypes[1] = new AttrType(AttrType.attrString);  // Department
        innerAttrTypes[2] = new AttrType(AttrType.attrInteger); // Salary
        
        short[] outerStrSizes = new short[1];
        outerStrSizes[0] = 20;
        
        short[] innerStrSizes = new short[1];
        innerStrSizes[0] = 20;

        //create proj list for flieecan
        FldSpec[] proj_list_out = new FldSpec[3];
        proj_list_out[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list_out[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list_out[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        
        // Create a file scan on the outer relation
        FileScan outerScan = new FileScan(
            outerFileName, 
            outerAttrTypes, 
            outerStrSizes,
            (short) 3, 
            3, 
            proj_list_out, 
            null
        );


        
        // Set up the join condition (R.ID = S.ID)
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = null;
        
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        // outFilter[0].type1 = new Operand();
        // outFilter[0].type2 = new Operand();
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1 = new Operand();
        outFilter[0].operand2 = new Operand();
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        
        // Set up the projection list
        FldSpec[] projList = new FldSpec[5];
        projList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);    // outer.ID
        projList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);    // outer.Name
        projList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);    // outer.Age
        projList[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2); // inner.Department
        projList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3); // inner.Salary
        
        // Create the index nested loop join iterator
        IndexType in = new IndexType(1);
        INLJoins inlJoin = new INLJoins(
            outerAttrTypes,
            3,
            outerStrSizes,
            innerAttrTypes,
            3,
            innerStrSizes,
            NUM_PAGES,
            outerScan,
            innerFileName,
            in,
            indexFileName,
            outFilter,
            null,
            projList,
            5
        );
        
        // Print header
        System.out.println("\nResults of Index Nested Loop Join (R.ID = S.ID):");
        System.out.println("ID | Name | Age | Department | Salary");
        System.out.println("-----------------------------------------");
        
        // Iterate through the results
        Tuple tuple;
        int count = 0;
        
        // Set up the attribute types for the join result
        AttrType[] joinAttrTypes = new AttrType[5];
        joinAttrTypes[0] = new AttrType(AttrType.attrInteger); // outer.ID
        joinAttrTypes[1] = new AttrType(AttrType.attrString);  // outer.Name
        joinAttrTypes[2] = new AttrType(AttrType.attrInteger); // outer.Age
        joinAttrTypes[3] = new AttrType(AttrType.attrString);  // inner.Department
        joinAttrTypes[4] = new AttrType(AttrType.attrInteger); // inner.Salary
        
        short[] joinStrSizes = new short[2];
        joinStrSizes[0] = 20; // size for outer.Name
        joinStrSizes[1] = 20; // size for inner.Department
        
        while ((tuple = inlJoin.get_next()) != null) {
            tuple.setHdr((short) 5, joinAttrTypes, joinStrSizes);
            
            int id = tuple.getIntFld(1);
            String name = tuple.getStrFld(2);
            int age = tuple.getIntFld(3);
            String dept = tuple.getStrFld(4);
            int salary = tuple.getIntFld(5);
            
            System.out.println(id + " | " + name + " | " + age + " | " + dept + " | " + salary);
            count++;
        }
        
        System.out.println("\nTotal " + count + " records found.");
        
        // Clean up
        inlJoin.close();
        outerScan.close();
    }
    
    /**
     * Main testing method
     */
    public static void main(String[] args) {
        try {
            // Initialize the MiniBase system
            String dbpath = "/tmp/inljtest_db"; 
            SystemDefs sysdef = new SystemDefs(dbpath, 4096, NUM_PAGES, "Clock");
            
            String outerFileName = "outrel";
            String innerFileName = "inrel";
            String indexFileName = "inrelbtree";
            
            // Create sample data
            createSampleData(outerFileName, innerFileName);
            
            // Create index on inner relation
            createBTreeIndex(innerFileName, indexFileName);
            
            // Test standard nested loops join first (for comparison)
            //runNestedLoopJoin(outerFileName, innerFileName);
            
            // Now test index nested loops join
            runINLJoin(outerFileName, innerFileName, indexFileName);
            
            // Measure and print buffer manager statistics
            System.out.println("\nBuffer Manager Statistics:");
            System.out.println("Number of disk reads: " + 
                              PCounter.rcounter);
            System.out.println("Number of disk writes: " + 
                              PCounter.wcounter);
            
            // Clean up
            SystemDefs.JavabaseDB.closeDB();
            
        } catch (Exception e) {
            System.err.println("Error occurred during testing: " + e);
            e.printStackTrace();
        }
    }
}