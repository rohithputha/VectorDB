package btree;

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

public class btreerealtest {
    public static void main(String[] args) throws Exception {
    
        SystemDefs sysdef = new SystemDefs("sdf1", 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        BTreeFile realIndex = new BTreeFile("RealIndexFile", AttrType.attrReal, 8, DeleteFashion.NAIVE_DELETE);
        Heapfile baseFile = new Heapfile("sdf1");
        Scan scan = baseFile.openScan();
        RID rid = new RID();

        PageId pid = SystemDefs.JavabaseDB.get_file_entry("handLsdf1");
        HFPage page = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pid, page, false);
        Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
        m.setHdr((short) 3,
                new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                null);
        int h = m.getIntFld(1);
        int L = m.getIntFld(2);
        int input_fields_length = m.getIntFld(3);

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

        for(AttrType i: schemaAttrTypes){
            System.out.println(i);
        }

        Tuple tuple;
        while ((tuple = scan.getNext(rid)) != null) {
            tuple.setHdr((short) input_fields_length, schemaAttrTypes, null);
            Float realValue = tuple.getFloFld(1);
            RealKey key = new RealKey(realValue);
            realIndex.insert(key, rid);
        }
        scan.closescan();
        //SystemDefs.JavabaseBM.flushAllPages();

        Heapfile hf = new Heapfile("sdf1");
        BTFileScan bscan = realIndex.new_scan(null, null);
        KeyDataEntry entry;
        while ((entry = bscan.get_next()) != null) {
            RealKey rKey = (RealKey) entry.key;
            System.out.println("Key: " + rKey.getKey() + " | Data: " + entry.data);
            RID foundRid = ((LeafData) entry.data).getData();
            Tuple foundTuple = hf.getRecord(foundRid);
            foundTuple.setHdr((short) input_fields_length, schemaAttrTypes, null);
            System.out.println(foundTuple.getFloFld(1));
            // process foundTuple
        }
        bscan.DestroyBTreeFileScan();

        SystemDefs.JavabaseDB.closeDB();

        


    }
}
