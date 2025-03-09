package LSHFIndex;

import global.*;
import heap.*;

import java.util.List;


public class LSHFIndexLeafPageTraversalTest {
    public static void main(String[] args) {
        try {
            // Step 1: Create a new database
            String dbpath = "testdb";
            int numPages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
            SystemDefs sysdef = new SystemDefs(dbpath, GlobalConst.MINIBASE_DB_SIZE, numPages, "Clock");
            System.out.println("MiniBase initialized with " + numPages + " buffer pages");

            // Step 2: Insert 5 tuples into the database using LSHFIndexFile
            LSHFIndexFile index = new LSHFIndexFile("testIndex", 5, 10); // 10 hash functions, 10 layers
            System.out.println("Index File Created");

            // Create test vectors and RIDs
            Vector100Dtype key1 = createVector(1);
            Vector100Dtype key2 = createVector(2);
//            Vector100Dtype key3 = createVector(3);
//            Vector100Dtype key4 = createVector(4);
//            Vector100Dtype key5 = createVector(5);
            System.out.println("Vectors Created successfully");

            index.insert(key1, new RID(new PageId(1), 1));
            System.out.println("Vectors 1 inserted successfully");
            index.insert(key2, new RID(new PageId(2), 3));
            System.out.println("Vectors 2 inserted successfully");
            //index.insert(key3, new RID(new PageId(1), 3));
            //index.insert(key4, new RID(new PageId(1), 4));
            //index.insert(key5, new RID(new PageId(1), 5));
            //System.out.println("Inserted 5 tuples into LSHFIndexFile");

            // Step 3: Inspect layer 0's start page structure



            LSHLayerMap layerMap = LSHLayerMap.getInstance();
            LSHLayer layer = layerMap.getLayerByLayerId(0); // Directly access layer 0
            if (layer == null) {
                throw new Exception("Layer 0 not found in LSHLayerMap");
            }
            int startPageId = layer.getLayerStartPage();
            System.out.println("Selected layer 0 with start page ID: " + startPageId);

            PageId pid = new PageId(startPageId);
//            LSHFInnerPage layerRootPage = new LSHFInnerPage(pid);
            LSHBasePage layerRootBasePage = new LSHBasePage(pid);
            // List<Integer> leaf_Page_Ids = new ArrayList<>();
            // System.out.println("Tye of the page is: " + layerRootPage.getPageType());
            // int NextPageId = -1;
            // for(short i=2; i<layerRootPage.getSlotCnt(); i++){
            //     Tuple u = layerRootPage.getRecord(new RID(layerRootPage.getCurPage(), i));
            //         u.setHdr((short)2,
            //             new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
            //             null);

            //         NextPageId = u.getIntFld(2);
            //         leaf_Page_Ids.add(NextPageId);
            //         //System.out.println(NextPageId);

            // }
            // for(int i=0;i<leaf_Page_Ids.size();i++){
            //     System.out.println(leaf_Page_Ids.get(i));
            // }
            //int hashFuncIndex = layerRootPage.getHashFunctionInConsideration();

            //pid = new PageId(37);
            //analyzepage(pid);//Leaf page!
            //System.out.println(hashFuncIndex);



            List<LSHDto> leafPageIds = index.collectLeafPageIds(layerRootBasePage);
            for(LSHDto pair: leafPageIds){
                System.out.println(pair.getPid() + " " + pair.getSid());
            }
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

    public static void analyzepage(PageId pid) throws Exception{
        LSHBasePage page = new LSHBasePage(pid);
        page.dumpPage();
        RID rid = page.firstRecord();
        int slotNum = 0;
        System.out.println("SLOT NO " + rid.slotNo);
        Tuple t = page.getRecord(rid);
        try {
            t.setHdr((short)1, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            int type_of_page = t.getIntFld(1);
            //int layer_id = t.getIntFld(2);
            System.out.println("Type of page: " + type_of_page);
            if(type_of_page==5){
                int firstBucketPageId = -1;
                for (short i = 2; i < page.getSlotCnt(); i++) {
                    Tuple u = page.getRecord(new RID(page.getCurPage(), i));
                    u.setHdr((short)2,
                            new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                            null);

                    firstBucketPageId = u.getIntFld(2);
                    break;
                }
                System.out.println("Next page Id to follow " + firstBucketPageId);

            }
            else if(type_of_page==4){
                //LSHFLeafPage lfp = new LSHFLeafPage(pid);

                // LSHFPage lp = new LSHFPage(pid);
                // lp.dumpPage();
                Tuple v = page.getRecord(new RID(page.getCurPage(),2));
                v.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
                int data_id = v.getIntFld(2);
                System.out.println("Record is points to this page no: " + data_id);



            }
        } catch (Exception e1) {
            System.out.println("  Unknown metadata format: " + e1.getMessage());
        }
        // int firstBucketPageId = -1;
        // for (short i = 2; i < page.getSlotCnt(); i++) {
        //     Tuple u = page.getRecord(new RID(page.getCurPage(), i));
        //     u.setHdr((short)2,
        //         new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
        //         null);

        //     firstBucketPageId = u.getIntFld(2);
        //     break;
        // }
        // System.out.println("Next page Id to follow " + firstBucketPageId);
        //PageId nextpid = new PageId(firstBucketPageId);
        //return nextpid;
    }




}