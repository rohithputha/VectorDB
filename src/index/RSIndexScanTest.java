package index;

import java.util.ArrayList;
import java.util.List;

import LSHFIndex.LSHBasePage;
import LSHFIndex.LSHDto;
import LSHFIndex.LSHFIndexFile;
import LSHFIndex.LSHFInnerPage;
import LSHFIndex.LSHLayer;
import LSHFIndex.LSHLayerMap;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.*;

public class RSIndexScanTest {
    public static void main(String[] args) throws Exception {
        String dbpath = "mydb";
        int num_pages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
        SystemDefs sysdef = new SystemDefs(dbpath, 0, 50, "Clock");
        System.out.println("MiniBase initialized with " + num_pages + " buffer pages");

//         //Create tuples
//        AttrType[] types = {new AttrType(AttrType.attrVector100D)};
//        Tuple t1 = new Tuple(206);
//        t1.setHdr((short)1, types, null);
//        t1.set100DVectFld(1, createVector(1));
//        System.out.println("t1 field type: " + types[0].attrType);
//        short[] t1Vals = t1.get100DVectFld(1).getVector();
//        System.out.println("t1 First: " + t1Vals[0] + ", Last: " + t1Vals[99]);

//        Tuple t2 = new Tuple(206);
//        t2.setHdr((short)1, types, null);
//        short[] vals2 = new short[100];
//        for (int i = 0; i < 100; i++) vals2[i] = (short)(i + 10);
//        t2.set100DVectFld(1,createVector(2));
//        System.out.println("t2 field type: " + types[0].attrType);
//        short[] t2Vals = t2.get100DVectFld(1).getVector();
//        System.out.println("t2 First: " + t2Vals[0] + ", Last: " + t2Vals[99]);

//        Tuple t3 = new Tuple(206);
//        t3.setHdr((short)1, types, null);
//        short[] vals3 = new short[100];
//        for (int i = 0; i < 100; i++) vals3[i] = (short)(i + 20);
//        t3.set100DVectFld(1, createVector(3));
//        System.out.println("t3 field type: " + types[0].attrType);
//        short[] t3Vals = t3.get100DVectFld(1).getVector();
//        System.out.println("t3 First: " + t3Vals[0] + ", Last: " + t3Vals[99]);



//        // Create heap file
//        Heapfile hf = new Heapfile("test.heap");
//        RID rid1 = hf.insertRecord(t1.getTupleByteArray());
//        RID rid2 = hf.insertRecord(t2.getTupleByteArray());
//        RID rid3 = hf.insertRecord(t3.getTupleByteArray());
//        System.out.println("Inserted " + hf.getRecCnt() + " tuples into test.heap");

//         //------------------------------------------------------------------------------------------------------

//        int numPages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
//        System.out.println("MiniBase initialized with " + numPages + " buffer pages");

//        // Step 2: Insert 5 tuples into the database using LSHFIndexFile
//        LSHFIndexFile index = new LSHFIndexFile("testdb1_10_10", 10, 10); // 10 hash functions, 10 layers
//        System.out.println("Index File Created");

//        // Create test vectors and RIDs
//        Vector100Dtype key1 = createVector(1);
//        Vector100Dtype key2 = createVector(2);
//            Vector100Dtype key3 = createVector(3);
// //            Vector100Dtype key4 = createVector(4);
// //            Vector100Dtype key5 = createVector(5);
//        System.out.println("Vectors Created successfully");

//        index.insert(key1, rid1);
//        System.out.println("Vectors 1 inserted successfully");
//        index.insert(key2, rid2);
//        System.out.println("Vectors 2 inserted successfully");
//        index.insert(key3, rid3);
//        System.out.println("Vectors 3 inserted successfully");

//         //index.insert(key3, new RID(new PageId(1), 3));
//         //index.insert(key4, new RID(new PageId(1), 4));
//         //index.insert(key5, new RID(new PageId(1), 5));
//         //System.out.println("Inserted 5 tuples into LSHFIndexFile");

//         //Step 3: Inspect layer 0's start page structure



//        LSHLayerMap layerMap = LSHLayerMap.getInstance("testdb1_1_1");
//        LSHLayer layer = layerMap.getLayerByLayerId(0); // Directly access layer 0
//        if (layer == null) {
//            throw new Exception("Layer 0 not found in LSHLayerMap");
//        }
//        int startPageId = layer.getLayerStartPage();
//        System.out.println("Selected layer 0 with start page ID: " + startPageId);

//        PageId pid = new PageId(startPageId);
//        LSHFInnerPage layerRootPage = new LSHFInnerPage(pid);
//        LSHBasePage layerRootBasePage = new LSHBasePage(pid,"testIndex");
//         List<Integer> leaf_Page_Ids = new ArrayList<>();
//         System.out.println("Tye of the page is: " + layerRootPage.getPageType());
//         int NextPageId = -1;
//         for(short i=2; i<layerRootPage.getSlotCnt(); i++){
//             Tuple u = layerRootPage.getRecord(new RID(layerRootPage.getCurPage(), i));
//                 u.setHdr((short)2,
//                     new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
//                     null);

//                 NextPageId = u.getIntFld(2);
//                 leaf_Page_Ids.add(NextPageId);
//                 //System.out.println(NextPageId);

//         }
//         for(int i=0;i<leaf_Page_Ids.size();i++){
//             System.out.println(leaf_Page_Ids.get(i));
//         }
//         int hashFuncIndex = layerRootPage.getHashFunctionInConsideration();

//         //pid = new PageId(37);
//         //analyzepage(pid);//Leaf page!
//         System.out.println(hashFuncIndex);



//        List<LSHDto> leafPageIds = index.collectLeafPageIds(layerRootBasePage);
//        for(LSHDto pair: leafPageIds){
//            System.out.println(pair.getPid() + " " + pair.getSid());
//        }
//        SystemDefs.JavabaseDB.closeDB();
//        System.out.println("Test completed.");

//        Sort sort = index.rangeScan(createVector(1), 2);
//        Tuple result;
//        int count = 0;
//        while ((result = sort.get_next()) != null && count < 300) {
//            System.out.println("Result " + (count + 1) + ": First: " + result.get100DVectFld(1).get(1) + ", Last: " + result.get100DVectFld(1).get(99));
//            count++;
//        }
//     }
        RSIndexScan rIndexScan= new RSIndexScan(
                new IndexType(IndexType.LSHFIndex),
                "mydb",
                "mydb3_5_5",
                new AttrType[]{new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D)},
                null,
                4,
                4,
                new FldSpec[]{new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3), new FldSpec(new RelSpec(RelSpec.outer), 4)},
                null,
                2,
                createVector(2),
                6
        );
        while(true){
            Tuple t = rIndexScan.get_next();
            if (t != null) {
                t.setHdr((short)4, new AttrType[]{new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D)}, null);
                Vector100Dtype v = t.get100DVectFld(4);
                for (int i=0;i<100;i++){
                    System.out.print(v.get(i)+", ");
                }
                System.out.println();
            }
            else break;
        }
        rIndexScan.close();
    }

    private static Vector100Dtype createVector(int base) {
        short[] values = new short[100];
        for (int i = 0; i < 100; i++) {
            values[i] = (short) (base + i);
        }
        return new Vector100Dtype(values);
    }
}
