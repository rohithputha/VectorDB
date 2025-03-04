package LSHFIndex;

import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import java.io.*;

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
            LSHLayer layer = layerMap.getLayerByLayerId(0); // Directly access layer 0
            if (layer == null) {
                throw new Exception("Layer 0 not found in LSHLayerMap");
            }
            int startPageId = layer.getLayerStartPage();
            System.out.println("Selected layer 0 with start page ID: " + startPageId);

            // Examine the page structure
            examinePageStructure(startPageId);

            // Cleanup
            index.close();
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

    // Helper: Examine the structure of a page
    private static void examinePageStructure(int pageId) throws Exception {
        HFPage page = new HFPage();
        PageId pid = new PageId(pageId);
        SystemDefs.JavabaseBM.pinPage(pid, page, false);

        System.out.println("Examining page " + pageId + ":");
        System.out.println("Slot count: " + page.getSlotCnt());
        page.dumpPage();
        System.out.println("Next page: " + page.getNextPage().pid);

        // Iterate through all slots
        RID rid = page.firstRecord();
        int slotNum = 0;
        while (rid != null) {
            System.out.println("Slot " + slotNum + " (RID: " + rid.slotNo + "):");
            Tuple t = page.getRecord(rid);
            if (t != null) {
                try {
                    // Try leaf metadata format: [type, numVectors]
                    t.setHdr((short)2, new AttrType[]{
                        new AttrType(AttrType.attrInteger),
                        new AttrType(AttrType.attrInteger)
                    }, null);
                    short type = (short) t.getIntFld(1);
                    int numVectors = t.getIntFld(2);
                    System.out.println("  Leaf format - Type: " + type + ", NumVectors: " + numVectors);
                } catch (Exception e1) {
                    try {
                        // Try inner metadata format: [type, hashInConsideration, layerId]
                        t.setHdr((short)3, new AttrType[]{
                            new AttrType(AttrType.attrInteger),
                            new AttrType(AttrType.attrInteger),
                            new AttrType(AttrType.attrInteger)
                        }, null);
                        short type = (short) t.getIntFld(1);
                        int hashInConsideration = t.getIntFld(2);
                        int layerId = t.getIntFld(3);
                        System.out.println("  Inner format - Type: " + type + ", HashInConsideration: " + hashInConsideration + ", LayerId: " + layerId);
                    } catch (Exception e2) {
                        try {
                            // Try data tuple format: [vector, pageNo, slotNo]
                            t.setHdr((short)3, new AttrType[]{
                                new AttrType(AttrType.attrVector100D),
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrInteger)
                            }, null);
                            Vector100Dtype vector = t.get100DVectFld(1);
                            int pageNo = t.getIntFld(2);
                            int slotNo = t.getIntFld(3);
                            //System.out.println("  Data tuple - Vector: [length=" + vector.data.length + "], PageNo: " + pageNo + ", SlotNo: " + slotNo);
                        } catch (Exception e3) {
                            System.out.println("  Unknown tuple format");
                        }
                    }
                }
            } else {
                System.out.println("  No tuple data available");
            }
            slotNum++;
            rid = page.nextRecord(rid);
        }

        SystemDefs.JavabaseBM.unpinPage(pid, false);
    }
}