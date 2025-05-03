package cli;

import LSHFIndex.LSHFIndexFile;
import btree.*;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static global.GlobalConst.INVALID_PAGE;

public class BatchDeleteCommand implements VectorDbCommand{

    private static class DataFileMetaData{
//        private DataFileMetaData instance;
        private int n;
        private AttrType[] attrs;
        private List<Integer> vectorFields;

        public int getN() {
            return n;
        }
        public AttrType[] getAttrs() {
            return attrs;
        }
        public AttrType getAttr(int idx) {
            return attrs[idx];
        }
        public List<Integer> getVectorFields() {
            return vectorFields;
        }
        private DataFileMetaData(int n, AttrType[] attrs, List<Integer> vectorFields){
            this. n = n;
            this.attrs = attrs;
            this.vectorFields = vectorFields;
        }
        public static DataFileMetaData getInstance(BufferedReader reader) throws Exception{
            try{
                int n = Integer.parseInt(reader.readLine().trim());
                AttrType[] attrs = new AttrType[n];
                List<Integer> vectorFields = new ArrayList<Integer>();
                String[] typeStrs = reader.readLine().trim().split("\\s+");
                for (int i = 0; i < n; i++){
                    int typeInt = Integer.parseInt(typeStrs[i]);
                    switch(typeInt) {
                        case 1:
                            attrs[i] = new AttrType(AttrType.attrInteger);
                            break;
                        case 2:
                            attrs[i] = new AttrType(AttrType.attrReal);
                            break;
                        case 3:
                            attrs[i] = new AttrType(AttrType.attrString);
                            break;
                        case 4:
                            attrs[i] = new AttrType(AttrType.attrVector100D);
                            vectorFields.add(i+1);
                            break;
                        default:
                            throw new Exception("Unknown attribute type: " + typeInt);
                    }
                }
                return new DataFileMetaData(n, attrs, vectorFields);
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception("error parsing datafile.");

            }

        }
    }

    public static final String COMMAND = "batchdelete";
    private String command;
    private String fileName;
    private String relName;
    private BufferedReader reader;
    private DataFileMetaData dataFileMetaData;
    private Map<Integer, LSHFIndexFile> lshfIndexFileMap;
    private Map<Integer, BTreeFile> btreeFileMap;
    public BatchDeleteCommand(String[] args) {
        this.command = args[0];
        this.fileName = args[1];
        this.relName = args[2];
        try{
            this.reader = new BufferedReader(new FileReader(args[1]));
        }
        catch (Exception e){
            printer("Error: " + e.getMessage());
        }
    }
    @Override
    public String getCommand() {
        return this.command;
    }

    private void initMetadataObject() throws Exception {
        this.dataFileMetaData = DataFileMetaData.getInstance(this.reader);
    }

    private void initLshfIndexFileMap() throws Exception {
        this.lshfIndexFileMap = new HashMap<Integer, LSHFIndexFile>();
        PageId hLPid = SystemDefs.JavabaseDB.get_file_entry("handL" + this.relName);
        if (hLPid == null || hLPid.pid == INVALID_PAGE){
            return;
        }

        HFPage hLHeapPage = new HFPage();
        SystemDefs.JavabaseBM.pinPage(hLPid, hLHeapPage, false);
        for (int i = 0;i < hLHeapPage.getSlotCnt(); i++){
            Tuple t = hLHeapPage.getRecord(new RID(hLPid, i));
            t.setHdr((short) 3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                    null);

            if(this.dataFileMetaData.getVectorFields().contains(t.getIntFld(3))){
                String indexName = this.relName + "_" + t.getIntFld(3) + "_" + t.getIntFld(2) + "_" + t.getIntFld(1);
                this.lshfIndexFileMap.put(t.getIntFld(3), new LSHFIndexFile(indexName));
            }
        }

        SystemDefs.JavabaseBM.unpinPage(hLPid, false);
    }

    private void initBtreeFileMap() throws Exception {
        this.btreeFileMap = new HashMap<Integer, BTreeFile>();
        // do after there is a clear info where the indexes are there...
        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + this.relName);
        HFPage page = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pid, page, false);
        Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
        m.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
        int input_fields_length = m.getIntFld(1);

        System.out.println(input_fields_length);

        AttrType[] intattrarray = new AttrType[input_fields_length];

        for (int i = 0; i < input_fields_length; i++) {
            intattrarray[i] = new AttrType(AttrType.attrInteger);
        }

        Tuple f = page.getRecord(new RID(page.getCurPage(), 1));
        f.setHdr((short) input_fields_length,
                intattrarray,
                null);

        //AttrType[] schemaAttrTypes = new AttrType[input_fields_length];


        for (int i = 0; i < input_fields_length; i++) {
            int fldtype = f.getIntFld(i + 1);
            if (fldtype>0 && fldtype<4) {
                //schemaAttrTypes[i] = new AttrType(AttrType.attrInteger);
                String btree_index_name = this.relName + "_" + (i+1);
                //System.out.println(btree_index_name);
                try{
                    BTreeFile btf = new BTreeFile(btree_index_name);
                    this.btreeFileMap.put(i, btf);
                    //System.out.println("--------------------------->");
                    //System.out.println(i+" "+btf);
                }catch(Exception e){
                    System.out.println(btree_index_name);
                    e.printStackTrace();
                    printer(e.getMessage());
                    //exit
                    //continue;
                }
            }
        }

        SystemDefs.JavabaseBM.unpinPage(pid, false);




    }

    private void deleteRidsLsh(Heapfile relHeapFile, FileScan fs, AttrType[] attrTypes) throws Exception {
        Set<String> rids = new HashSet<>();
        while(true){
            Tuple t = fs.get_next();
            if (t == null){
                break;
            }
            t.setHdr((short) (attrTypes.length), attrTypes,null);
            String ridString = t.getIntFld(2)+"_"+t.getIntFld(3);
            if (rids.contains(ridString)){
                continue;
            }
            RID rid = new RID(new PageId(t.getIntFld(2)), t.getIntFld(3));
            relHeapFile.deleteRecord(rid);
            rids.add(ridString);
        }
    }

    private List<RID> deleteRidsBtree(Heapfile relHeapFile, BTFileScan fs) throws Exception {
        List<RID> rids = new ArrayList<>();
        System.out.println("Entering delete rids Btree------");
        while(true){
            System.out.println(fs.toString());
            KeyDataEntry t= fs.get_next();
            System.out.println(t);
            if (t == null){
                break;
            }
            System.out.println("########################");
            System.out.println(t.key);

            LeafData ld = (LeafData) t.data;

            RID rid = new RID(ld.getData().pageNo, ld.getData().slotNo);
            relHeapFile.deleteRecord(rid);
            rids.add(rid);
        }
        return rids;
    }

    private boolean checkEquality(int attrNum, Tuple t, Object delKey) throws Exception {
        //Tuple x = new Tuple();
        // AttrType[] attrs = new AttrType[this.dataFileMetaData.getN()];

        // for(int i=0;i<this.dataFileMetaData.getN();i++){
        //     attrs[i] = this.dataFileMetaData.getAttr(i);
        //     //System.out.println(this.dataFileMetaData.getAttr(i));
        // }
        // t.setHdr( (short) this.dataFileMetaData.getN(), attrs, null);
        // System.out.println(this.dataFileMetaData.getN());
        //System.out.println(delKey);
        if(this.dataFileMetaData.getAttr(attrNum-1).attrType == 1){
            Integer k = (Integer) delKey;
            return t.getIntFld(attrNum)== k;
        }
        else if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 2){
            // System.out.println("-------------------->");
            // // System.out.println(attrNum);
            Float d = Float.parseFloat(delKey.toString());
            // System.out.println(d);
            // System.out.println(t.getFloFld(attrNum));
            return t.getFloFld(attrNum)==d;
        }
        else if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 0){
            String s = (String) delKey;
            return t.getStrFld(attrNum).equals(s);
        }
        else if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 5){
            Vector100Dtype v = (Vector100Dtype) delKey;
            return t.get100DVectFld(attrNum).distanceTo(v) == 0 ;
        }
        return false;
    }
    private void deleteHeapFile(FileScan fs, Heapfile hf,AttrType[] attrTypes, int attrNum, Object delKey) throws Exception {
        List<RID> rids = new ArrayList<>();


        while(true){
            RID nrid = fs.get_rid();
            Tuple t = fs.get_next();
            if (t == null){
                break;
            }
            t.setHdr((short) (attrTypes.length), attrTypes,null);
            if (checkEquality(attrNum,t, delKey)){
                // System.out.println(delKey);
                // System.out.println("checked ---- passed");
                // System.out.println(t.getFloFld(3));

                // System.out.println();
                //System.out.println("Going here ---------->");

                //System.out.println(nrid.pageNo.pid+" "+nrid.slotNo );
                if(nrid != null){
                    rids.add(nrid);
                }


            }
        }
        // Heapfile baseFile = new Heapfile(this.relName);

        // Scan scan = baseFile.openScan();
        // RID rid = new RID();
        // //int cnt = 0;
        // Tuple tuple = new Tuple();
        // while ((tuple = scan.getNext(rid)) != null){
        //     tuple.setHdr((short) (attrTypes.length), attrTypes,null);
        //     //Float intValue = tuple.getFloFld(3);
        //     //System.out.println(intValue);
        //     if (checkEquality(attrNum,tuple, delKey)){
        //         //System.out.println(delKey);
        //         //System.out.println("checked ---- passed");
        //         //System.out.println(tuple.getFloFld(3));
        //         //RID nrid = fs.get_rid();
        //         //System.out.println(nrid.pageNo.pid+" "+nrid.slotNo );
        //         //System.out.println();
        //         if(rid!=null){
        //             rids.add(scan.getUserrid());
        //         }

        //     }

        // }
        // scan.closescan();



        for (RID ridx: rids){
            //System.out.println(ridx.slotNo);
            hf.deleteRecord(ridx);

        }
        return;
    }

    @Override
    public void process() {
        try{
            initMetadataObject();
            initLshfIndexFileMap();
            initBtreeFileMap();
            if (this.getEnvironment().getDb() != null){
                Heapfile relHeapfile = new Heapfile(this.relName);
                while(true){
                    String line = reader.readLine();
                    if (line == null){
                        break;
                    }
                    //System.out.println(line);
                    String[] ds = line.split("\\s+");
                    // for(String i: ds){
                    //     System.out.println(i);
                    // }
                    int attrNum = Integer.parseInt(ds[0]);
                    //System.out.println(this.dataFileMetaData.getAttr(attrNum-1).attrType);
                    if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 5){
                        System.out.println("%%%%%%%%%%%%%%%%Going here%%%%%%%%%%%%%%%%%%%%%%%");
                        short[] s = new short[100];
                        for (int i = 0;i<100;i++){
                            s[i] = Short.parseShort(ds[i+1]);
                        }
                        Vector100Dtype deleteKey = new Vector100Dtype(s);
                        if(this.lshfIndexFileMap.containsKey(attrNum)){
                            Random random = new Random();
                            String tempHfName = this.relName+""+random.nextInt(100)+""+attrNum;
                            Heapfile tempHf  = new Heapfile(tempHfName);
                            LSHFIndexFile lshfIndexFile = this.lshfIndexFileMap.get(attrNum);
                            lshfIndexFile.delete(deleteKey, tempHf);
                            FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
                            AttrType[] types = {new AttrType(AttrType.attrVector100D), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
                            FileScan fs = new FileScan(tempHfName, types, null, (short)3, (short)3, projlist, null);
                            this.deleteRidsLsh(relHeapfile, fs, types);
                            fs.close();
                            lshfIndexFile.close();
                            SystemDefs.JavabaseBM.flushAllPages();
                        }
                        else{
                            //while(true){

//                                FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
                            AttrType[] types = this.dataFileMetaData.getAttrs();
                            FldSpec[] projlist = new FldSpec[types.length];
                            for (int i = 0;i<types.length;i++){
                                projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
                            }
                            FileScan fs = new FileScan(this.relName, types, null, (short)types.length, (short)types.length, projlist, null);
                            deleteHeapFile(fs,relHeapfile,types,attrNum, deleteKey);
                            SystemDefs.JavabaseBM.flushAllPages();
                            //}
                        }
                    }
                    else{
                        KeyClass delKey = null;
                        Object deleteKey = null;
                        //System.out.println("------------->");
                        //System.out.println(this.dataFileMetaData.getAttr(attrNum-1).attrType);
                        if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 1){
                            delKey = new IntegerKey(Integer.parseInt(ds[1]));
                            deleteKey =Integer.parseInt(ds[1]);
                        }
                        else if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 0){
                            delKey = new StringKey(ds[1]);
                            deleteKey =(ds[1]);
                        }
                        else if (this.dataFileMetaData.getAttr(attrNum-1).attrType == 2){
                            delKey = new RealKey(Float.parseFloat(ds[1]));
                            deleteKey =(ds[1]);
                        }

                        if(this.btreeFileMap.containsKey(attrNum-1)){
                            System.out.println("yohoyoho-------");
                            BTreeFile btreeFile = this.btreeFileMap.get(attrNum-1);
                            System.out.println(delKey);
                            BTFileScan bTreeFileScan = btreeFile.new_scan(delKey, delKey);
                            List<RID> rids  = this.deleteRidsBtree(relHeapfile, bTreeFileScan);
                            for (RID rid: rids){
                                System.out.println("***********");
                                System.out.println(delKey+" "+rid);
                                btreeFile.Delete(delKey,rid);
                            }
                            bTreeFileScan.DestroyBTreeFileScan();
                            btreeFile.close();
                            SystemDefs.JavabaseBM.flushAllPages();
                        }
                        else{
                            //while(true){
                            AttrType[] types = this.dataFileMetaData.getAttrs();
                            FldSpec[] projlist = new FldSpec[types.length];
                            for (int i = 0;i<types.length;i++){
                                projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
                            }
                            FileScan fs = new FileScan(this.relName, types, null, (short)types.length, (short)types.length, projlist, null);
                            deleteHeapFile(fs,relHeapfile,types,attrNum, deleteKey);
                            fs.close();
                            SystemDefs.JavabaseBM.flushAllPages();
                            //}
                        }



                    }

                    // if there is an index on top given column, use the column to get RIDs and delete from both index and heap file.
                    // else do a full heap file scan and delete the records found.

                }
            }
            else{
                printer("Error: database not opened.");
                return;
            }
        }
        catch(Exception e){
            e.printStackTrace();
            printer("Error :"+e.getMessage());
            return;
        }
    }
}