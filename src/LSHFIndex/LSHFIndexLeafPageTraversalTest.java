package LSHFIndex;

import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import java.io.*;

import LSHFIndex.LSHFInnerPage;
import LSHFIndex.LSHFLeafPage;

public class LSHFIndexLeafPageTraversalTest {
    public static void main(String[] args) {
        try {
            // Step 1: Create a new database
            String dbpath = "testdb";
            int numPages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
            SystemDefs sysdef = new SystemDefs(dbpath, GlobalConst.MINIBASE_DB_SIZE, numPages, "Clock");
            System.out.println("MiniBase initialized with " + numPages + " buffer pages");

            // Step 2: Insert 5 tuples into the database using LSHFIndexFile
            LSHFIndexFile index = new LSHFIndexFile("testIndex", 10, 10); // 10 hash functions, 10 layers
            System.out.println("Index File Created");

            // Create test vectors and RIDs
            Vector100Dtype key1 = createVector(1);
            Vector100Dtype key2 = createVector(2);
            Vector100Dtype key3 = createVector(3);
            Vector100Dtype key4 = createVector(4);
            Vector100Dtype key5 = createVector(5);
            System.out.println("Vectors Created successfully");

            index.insert(key1, new RID(new PageId(1), 1));
            System.out.println("Vectors 1 inserted successfully");
            index.insert(key2, new RID(new PageId(1), 2));
            index.insert(key3, new RID(new PageId(1), 3));
            index.insert(key4, new RID(new PageId(1), 4));
            index.insert(key5, new RID(new PageId(1), 5));
            System.out.println("Inserted 5 tuples into LSHFIndexFile");

            // Step 3: Inspect layer 0's start page structure
            LSHLayerMap layerMap = LSHLayerMap.getInstance();
            LSHLayer layer = layerMap.getLayerByLayerId(9); // Directly access layer 0
            if (layer == null) {
                throw new Exception("Layer 9 not found in LSHLayerMap");
            }
            int startPageId = layer.getLayerStartPage();
            System.out.println("Selected layer 9 with start page ID: " + startPageId);

            PageId pid = new PageId(startPageId);
            LSHFPage page = new LSHFPage(pid);
            System.out.println("Type " + page.getPageType);
            LSHFLeafPage lpage = new LSHFLeafPage(pid);
            System.out.println("Num of vectors:  " + lpage.getNumVectorInPage());


            // Examine the page structure

            // Cleanup
            //SystemDefs.JavabaseBM.unpinPage(startPageId, false);
            //index.close();
            SystemDefs.JavabaseDB.closeDB();
            System.out.println("Test completed.");

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Test failed: " + e.getMessage());
        }
    }

    // Helper: Create a simple Vector100Dtype for testing
    private static Vector100Dtype createVector(int base) {
        short[] values = new short[100];
        for (int i = 0; i < 100; i++) {
            values[i] = (short) (base + i);
        }
        return new Vector100Dtype(values);
    }

    
}