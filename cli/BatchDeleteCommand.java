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

    public static class DataFileMetaData{
        private static DataFileMetaData instance;
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
            if (instance != null){
                return instance;
            }
            try{
                int n = Integer.parseInt(reader.readLine().trim());
                AttrType[] attrs = new AttrType[n+1];
                List<Integer> vectorFields = new ArrayList<Integer>();
                String[] typeStrs = reader.readLine().trim().split("\\s+");
                for (int i = 1; i <= n; i++){
                    int typeInt = Integer.parseInt(typeStrs[i-1]);
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
                            vectorFields.add(i);
                            break;
                        default:
                            throw new Exception("Unknown attribute type: " + typeInt);
                    }
                }
                instance = new DataFileMetaData(n, attrs, vectorFields);
            } catch (Exception e) {
                throw new Exception("error parsing datafile.");
            }

            return instance;
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
        PageId hLPid = SystemDefs.JavabaseDB.get_file_entry("handL_" + this.relName);
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
        while(true){
            KeyDataEntry t= fs.get_next();
            if (t == null){
                break;
            }

            LeafData ld = (LeafData) t.data;
            RID rid = new RID(ld.getData().pageNo, ld.getData().slotNo);
            relHeapFile.deleteRecord(rid);
            rids.add(rid);
        }
        return rids;
    }

    private boolean checkEquality(int attrNum, Tuple t, Object delKey) throws FieldNumberOutOfBoundException, IOException {
        if(this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrInteger){
            Integer k = (Integer) delKey;
            return t.getIntFld(attrNum+1)== k;
        }
        else if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrReal){
            Double d = (Double) delKey;
            return t.getFloFld(attrNum+1)==d;
        }
        else if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrString){
            String s = (String) delKey;
            return t.getStrFld(attrNum+1).equals(s);
        }
        else if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrVector100D){
            Vector100Dtype v = (Vector100Dtype) delKey;
            return t.get100DVectFld(attrNum+1).distanceTo(v) == 0 ;
        }
        return false;
    }
    private void deleteHeapFile(FileScan fs, Heapfile hf,AttrType[] attrTypes, int attrNum, Object delKey) throws Exception {
        List<RID> rids = new ArrayList<>();

        while(true){
            Tuple t = fs.get_next();
            if (t == null){
                break;
            }
            t.setHdr((short) (attrTypes.length), attrTypes,null);
            if (checkEquality(attrNum,t, delKey)){
               rids.add(fs.get_rid());
            }
        }

        for (RID rid: rids){
            hf.deleteRecord(rid);
        }
        return;
    }

    @Override
    public void process() {
        try{
            initMetadataObject();
            initLshfIndexFileMap();
            if (this.getEnvironment().getDb() != null){
                Heapfile relHeapfile = new Heapfile(this.relName);
                while(true){
                    String line = reader.readLine();
                    if (line == null){
                        break;
                    }
                    String[] ds = line.split("\\s+");
                    int attrNum = Integer.parseInt(ds[0]);
                    if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrVector100D){
                        short[] s = new short[100];
                        for (int i = 0;i<100;i++){
                            s[i] = Short.parseShort(ds[i+1]);
                        }
                        Vector100Dtype deleteKey = new Vector100Dtype(s);
                        if(this.lshfIndexFileMap.containsKey(attrNum)){
                            Random random = new Random();
                            String tempHfName = this.relName+"_"+random.nextInt(100)+"_"+attrNum;
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
                            while(true){
                                FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
                                AttrType[] types = this.dataFileMetaData.getAttrs();
                                FileScan fs = new FileScan(this.relName, types, null, (short)3, (short)3, projlist, null);
                                deleteHeapFile(fs,relHeapfile,types,attrNum, deleteKey);
                            }
                        }
                    }
                    else{
                        KeyClass delKey = null;
                        Object deleteKey = null;
//                        if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrReal){
//                            delKey = (Double)Double.parseDouble(ds[1]);
//                        }
                        if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrInteger){
                           delKey = new IntegerKey(Integer.parseInt(ds[1]));
                           deleteKey =Integer.parseInt(ds[2]);
                        }
                        else if (this.dataFileMetaData.getAttr(attrNum).attrType == AttrType.attrString){
                           delKey = new StringKey(ds[1]);
                           deleteKey =(ds[2]);
                        }

                        if(this.btreeFileMap.containsKey(attrNum)){
                            BTreeFile btreeFile = this.btreeFileMap.get(attrNum);
                            BTFileScan bTreeFileScan = btreeFile.new_scan(delKey, delKey);
                            List<RID> rids  = this.deleteRidsBtree(relHeapfile, bTreeFileScan);
                            for (RID rid: rids){
                                btreeFile.Delete(delKey,rid);
                            }
                        }
                        else{
                            while(true){
                                FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1),new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
                                AttrType[] types = this.dataFileMetaData.getAttrs();
                                FileScan fs = new FileScan(this.relName, types, null, (short)3, (short)3, projlist, null);
                                deleteHeapFile(fs,relHeapfile,types,attrNum, deleteKey);
                            }
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
