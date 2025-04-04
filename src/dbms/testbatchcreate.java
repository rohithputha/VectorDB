package dbms;

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

public class testbatchcreate {
    

    public static void main(String[] args) throws Exception{
        SystemDefs sysdef = new SystemDefs("testdb", 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        Heapfile dataHeapFile = new Heapfile("testdb");
        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + "testdb");
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

        PageId pidhL = SystemDefs.JavabaseDB.get_file_entry("handL" + "testdb");
        HFPage page_x = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pidhL, page_x, false);
        Tuple x = page_x.getRecord(new RID(page_x.getCurPage(), 0));
        x.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)}, null);
        int h = x.getIntFld(1);
        int L = x.getIntFld(2);
        SystemDefs.JavabaseBM.unpinPage(pidhL, false);

        System.out.println(dataHeapFile.getRecCnt());
        System.out.println(input_fields_length);
        for(AttrType i: schemaAttrTypes){
            System.out.println(i);
        }
        System.out.println(h+" "+L);

        
        // Scan scan = dataHeapFile.openScan();
        // RID rid = new RID();
        // Tuple tuple = new Tuple();
        // tuple.setHdr((short) input_fields_length, schemaAttrTypes, null);
        
        // while ((tuple = scan.getNext(rid)) != null) {
        //     if (tuple != null) {
        //         tuple.setHdr((short) input_fields_length, schemaAttrTypes, null);
        //         Vector100Dtype vector = tuple.get100DVectFld(1 + 1);
        //         System.out.println(vector.get(0));
        //     }
        // }
        
        // scan.closescan();
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

            }
}
