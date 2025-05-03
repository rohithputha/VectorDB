package cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


import LSHFIndex.LSHFIndexFile;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.RealKey;
import btree.StringKey;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.*;
import diskmgr.PCounter;
import org.w3c.dom.Attr;
import diskmgr.PCounter;

import static global.GlobalConst.INVALID_PAGE;


public class BatchInsertCommand implements VectorDbCommand {


    private static class DataFileMetaData {
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

        private DataFileMetaData(int n, AttrType[] attrs, List<Integer> vectorFields) {
            this.n = n;
            this.attrs = attrs;
            this.vectorFields = vectorFields;
        }

        public static BatchInsertCommand.DataFileMetaData getInstance(BufferedReader reader) throws Exception {
            try {
                int n = Integer.parseInt(reader.readLine().trim());
                AttrType[] attrs = new AttrType[n];
                List<Integer> vectorFields = new ArrayList<Integer>();
                String[] typeStrs = reader.readLine().trim().split("\\s+");
                for (int i = 0; i < n; i++) {
                    int typeInt = Integer.parseInt(typeStrs[i]);
                    switch (typeInt) {
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
                            vectorFields.add(i + 1);
                            break;
                        default:
                            throw new Exception("Unknown attribute type: " + typeInt);
                    }
                }
                return new BatchInsertCommand.DataFileMetaData(n, attrs, vectorFields);
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception("error parsing datafile.");

            }

        }
    }

    private String commandName;
    private String DATAFILENAME;
    private String relName;
    public static final String COMMAND = "batchinsert";

    private BufferedReader reader;

    public BatchInsertCommand(String args[]) {
        this.commandName = args[0];
        this.DATAFILENAME = args[1];
        this.relName = args[2];
        try {
            this.reader = new BufferedReader(new FileReader(args[1]));
        } catch (Exception e) {
            printer("Error: " + e.getMessage());
        }
    }

    private Map<Integer, LSHFIndexFile> lshfIndexFileMap;
    private Map<Integer, BTreeFile> btreeFileMap;
    private DataFileMetaData dataFileMetaData;

    private void initLshfIndexFileMap() throws Exception {
        this.lshfIndexFileMap = new HashMap<Integer, LSHFIndexFile>();
        PageId hLPid = SystemDefs.JavabaseDB.get_file_entry("handL" + this.relName);
        if (hLPid == null || hLPid.pid == INVALID_PAGE) {
            return;
        }

        HFPage hLHeapPage = new HFPage();
        SystemDefs.JavabaseBM.pinPage(hLPid, hLHeapPage, false);
        for (int i = 0; i < hLHeapPage.getSlotCnt(); i++) {
            Tuple t = hLHeapPage.getRecord(new RID(hLPid, i));
            t.setHdr((short) 3, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                    null);

            if (this.dataFileMetaData.getVectorFields().contains(t.getIntFld(3))) {
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

        // System.out.println(input_fields_length);

        AttrType[] intattrarray = new AttrType[input_fields_length];

        for (int i = 0; i < input_fields_length; i++) {
            intattrarray[i] = new AttrType(AttrType.attrInteger);
        }

        Tuple f = page.getRecord(new RID(page.getCurPage(), 1));
        f.setHdr((short) input_fields_length,
                intattrarray, null);

        //AttrType[] schemaAttrTypes = new AttrType[input_fields_length];


        for (int i = 0; i < input_fields_length; i++) {
            int fldtype = f.getIntFld(i + 1);
            if (fldtype > 0 && fldtype < 4) {
                //schemaAttrTypes[i] = new AttrType(AttrType.attrInteger);
                String btree_index_name = this.relName + "_" + (i+1);
                //System.out.println(btree_index_name);
                try {
                    BTreeFile btf = new BTreeFile(btree_index_name);
                    this.btreeFileMap.put(i + 1, btf);
                    //System.out.println("--------------------------->");
                    //System.out.println(i+" "+btf);
                } catch (Exception e) {
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

    private void initMetadataObject() throws Exception {
        this.dataFileMetaData = BatchInsertCommand.DataFileMetaData.getInstance(this.reader);
    }


    @Override
    public String getCommand() {
        return this.commandName;
    }

    private Tuple tupleReader() throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
        Tuple tuple = new Tuple();
        AttrType[] attrs = dataFileMetaData.getAttrs();
        tuple.setHdr((short) attrs.length, attrs, null);
        for (int i = 0; i < attrs.length; i++) {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            switch (attrs[i].attrType) {
                case AttrType.attrInteger:
                    tuple.setIntFld(i + 1, Integer.parseInt(line.trim()));
                    break;
                case AttrType.attrReal:
                    tuple.setFloFld(i + 1, Float.parseFloat(line.trim()));
                    break;
                case AttrType.attrString:
                    tuple.setStrFld(i + 1, line);
                    break;
                case AttrType.attrVector100D:
                    short[] s = new short[100];
                    String[] ds = line.split("\\s+");
                    for (int j = 0; j < 100; j++) {
                        // s[j] = Short.parseShort((int)Float.parseFloat(ds[j]));
                        // s[j] = (short)Float.parseFloat(ds[j]);
                        Double decimalValue = Double.parseDouble(ds[j]);
                        s[j] = decimalValue.shortValue();

                    }
                    tuple.set100DVectFld(i + 1, new Vector100Dtype(s));
            }
        }
        return tuple;
    }

    private void insertIntoIndexes(Tuple t, RID rid, AttrType[] attrTypes) throws Exception {

        
        for (Map.Entry<Integer, LSHFIndexFile> entry : this.lshfIndexFileMap.entrySet()) {
            Integer qa = entry.getKey();
            LSHFIndexFile lshfIndexFile = entry.getValue();
            lshfIndexFile.insert(t.get100DVectFld(qa), rid);
        }
        for (Map.Entry<Integer, BTreeFile> entry : this.btreeFileMap.entrySet()) {
            Integer qa = entry.getKey();
            BTreeFile btreeFile = entry.getValue();
            switch (attrTypes[qa - 1].attrType) {
                case AttrType.attrInteger:
                    btreeFile.insert(new IntegerKey(t.getIntFld(qa)), rid);
                    break;
                case AttrType.attrReal:
                    btreeFile.insert(new RealKey(t.getFloFld(qa)), rid);
                    break;
                case AttrType.attrString:
                    btreeFile.insert(new StringKey(t.getStrFld(qa)), rid);
                    break;
            }
        }

    }

    @Override
    public void process() {

        long startTime = System.nanoTime();

        try {
            initMetadataObject();
            initLshfIndexFileMap();
            initBtreeFileMap();

            Heapfile hf = new Heapfile(this.relName);
            while (true) {
                Tuple t = tupleReader();
                if (t == null) {
                    break;
                }

                RID rid = hf.insertRecord(t.getTupleByteArray());
                insertIntoIndexes(t, rid, this.dataFileMetaData.getAttrs());
            }

            for (Map.Entry<Integer, LSHFIndexFile> entry : this.lshfIndexFileMap.entrySet()) {
                Integer qa = entry.getKey();
                LSHFIndexFile lshfIndexFile = entry.getValue();
                lshfIndexFile.close();
            }
            for (Map.Entry<Integer, BTreeFile> entry : this.btreeFileMap.entrySet()) {
                Integer qa = entry.getKey();
                BTreeFile btreeFile = entry.getValue();
                btreeFile.close();
            }

            SystemDefs.JavabaseBM.flushAllPages();

        } catch (Exception e) {
            e.printStackTrace();
            printer("Error while batch inserting :" + e.getMessage());

        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");
        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());


    }


}
