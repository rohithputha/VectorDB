package cli;

import LSHFIndex.LSHFIndexFile;
import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.*;
import index.NNIndexScan;
import index.RSIndexScan;
import iterator.*;

import javax.xml.validation.Schema;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RangeScanQueryCommand implements VectorDbCommand{
    private final String query;
    private final String COMMAND = "Range";
    private String relName1;
    private String relName2;
    private String queryBody;
    private boolean iter;
    private Iterator iterator;
    public RangeScanQueryCommand(String relName1, String relName2, String query, boolean iter) {
        this.query = query;
        this.relName1 = relName1;
        this.relName2 = relName2;
        this.iter = iter;
    }

    private int qa;
    private String targetFile;
    private Vector100Dtype t;
    private int d;
    private String i;
    private String j;
    private List<Integer> projectList;
    private AttrType qaAttributeType;
    private LSHFIndexFile lshfIndexFile;
    private String lshIndexName;
    private String getLshIndexName(String relName, int columnId) throws Exception {
        String fileName = "handL" + relName;
        PageId firstPid = SystemDefs.JavabaseDB.get_file_entry(fileName);
        if (firstPid == null) {
            throw new Exception("No LSH metadata found for relation: " + relName);
        }

        int L = -1, h = -1;
        PageId currentPid = firstPid;
        HFPage hfPage = new HFPage();

        // Scan all pages in the heap‐file for our columnId
        while (currentPid != null) {
            SystemDefs.JavabaseBM.pinPage(currentPid, hfPage, false);
            int slotCount = hfPage.getSlotCnt();
            for (int slot = 0; slot < slotCount; slot++) {
                RID rid = new RID(currentPid, slot);
                Tuple t = hfPage.getRecord(rid);
                // set header so we can read the 3 int fields: [L, h, columnId]
                t.setHdr((short)3,
                        new AttrType[]{
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrInteger)
                        },
                        null);
                if (t.getIntFld(3) == columnId) {
                    h = t.getIntFld(1);
                    L = t.getIntFld(2);
                    SystemDefs.JavabaseBM.unpinPage(currentPid, false);
                    break;
                }
            }
            if (L != -1) break;

            // move to next page (if any)
            PageId next = hfPage.getNextPage();
            SystemDefs.JavabaseBM.unpinPage(currentPid, false);
            currentPid = next;
        }

        if (L == -1) {
            throw new Exception("No metadata entry for column " + columnId +
                    " in relation " + relName);
        }

        // build and return index name
        return relName + "_" + columnId + "_" + L + "_" + h;
    }

    private Vector100Dtype getTaretVector(String filePath) throws Exception {
        short[] result = new short[100];
        int count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while (count < 100 && (line = br.readLine()) != null) {
                String[] tokens = line.trim().split("\\s+");
                for (String tok : tokens) {
                    if (count >= 100) break;
                    result[count++] = Short.parseShort(tok);
                }
            }
        }

        if (count != 100) {
            throw new IOException(
                    "Expected 100 numbers but found " + count + " in file: " + filePath
            );
        }

        return new Vector100Dtype(result);
    }

    private void parseQueryBody() throws Exception {
        String queryBody = this.query.split("[()]")[1];
        String[] args = queryBody.split(",");
        this.qa = Integer.parseInt(args[0].trim());
        this.qaAttributeType = getSchema(this.relName1)[this.qa-1];
        this.targetFile = args[1].trim();

        ///  Write logic to read the target file...

        this.d = Integer.parseInt(args[2].trim());
        this.i = args[3].trim();
        this.t = getTaretVector(this.targetFile);
        if (this.i.equalsIgnoreCase("H")) {
            try {
                this.lshIndexName = getLshIndexName(this.relName1, this.qa);
//                this.lshfIndexFile = new LSHFIndexFile(lshIndexName); /// need to find the h and l values for index name;
            } catch (Exception e) {
                this.lshfIndexFile = null;
            }
        }
        // projectList = new ArrayList<>();
        // for (int i = 4; i < args.length; i++) {
        //     projectList.add(Integer.parseInt(args[i].trim()));
        // }

        projectList = new ArrayList<>();
        if(args[4].trim().equals("*")){
            AttrType[] at1 = getSchema(this.relName1);
            for(int i=0;i<at1.length;i++){
                projectList.add(i+1);
            }
        }
        else{
            for (int i = 4; i < args.length; i++) {
                projectList.add(Integer.parseInt(args[i].trim()));
            } 
        }

    }


    private AttrType[] projectAttributeTypes = null;
    private int projQa = -1;
    private AttrType[] getProjectAttributeTypes(AttrType[] originalTypes, List<Integer> projectList) {
        if (projectAttributeTypes != null) {
            return projectAttributeTypes;
        }
        AttrType[] projAttrTypes = new AttrType[projectList.size()];
        for (int i = 0; i < projectList.size(); i++) {
            if (projectList.get(i) == this.qa) {
                this.projQa = i+1;
            }
            projAttrTypes[i] = originalTypes[projectList.get(i)-1];
            /// CHECK: 0 base or 1 based indexing
        }
        projectAttributeTypes = projAttrTypes;
        return this.projectAttributeTypes;
    }
    private Tuple projectTuple(Tuple original, AttrType[] originalTypes, List<Integer> projectList) throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
        Tuple t = new Tuple();
        AttrType[] projAttrTypes = getProjectAttributeTypes(originalTypes, projectList);
        t.setHdr((short) projAttrTypes.length, projAttrTypes, null);
        for (int i = 0; i < projectList.size(); i++) {
            switch (projAttrTypes[i].attrType) {
                case AttrType.attrInteger:
                    t.setIntFld(i+1,original.getIntFld(projectList.get(i)));
                    break;
                case AttrType.attrString:
                    t.setStrFld(i+1,original.getStrFld(projectList.get(i)));
                    break;
                case AttrType.attrReal:
                    t.setFloFld(i+1,original.getFloFld(projectList.get(i)));
                    break;
                case AttrType.attrVector100D:
                    t.set100DVectFld(i+1,original.get100DVectFld(projectList.get(i)));
                    break;
            }
        }
        return t;

    }

    private List<Tuple> useIndex() throws Exception {
//        PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL_" + this.relName1+"_"+this.qa);
//        HFPage page = new HFPage();
//        SystemDefs.JavabaseBM.pinPage(pid, page, false);
//        Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
//        m.setHdr((short) 3,
//                new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
//                null);
//        int h = m.getIntFld(1);
//        int L = m.getIntFld(2);



        AttrType[] schemaAttrTypes = getSchema(this.relName1);
        int inputFieldLength = schemaAttrTypes.length;
//        SystemDefs.JavabaseBM.unpinPage(pid, false);
//        String index_name = this.relName1 + Integer.toString(this.qa) + "_" + Integer.toString(h) + "_" + Integer.toString(L);
//                    String index_name = dbName + Integer.toString(QA-1)  + Integer.toString(h)  + Integer.toString(L);


        int output_fields_length = schemaAttrTypes.length;

        FldSpec[] outFldstack = new FldSpec[output_fields_length];
        for (int i = 0; i < output_fields_length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        System.out.println(lshIndexName);

        RSIndexScan rsIndexScan =  new RSIndexScan(
                new IndexType(IndexType.LSHFIndex),
                this.relName1,
                this.lshIndexName,
                schemaAttrTypes,
                null,
                inputFieldLength,
                output_fields_length,
                outFldstack,
                null,
                this.qa,
                this.t,
                this.d
        );

//        if (iter){
//            this.iterator = rsIndexScan;
//        }
        // System.out.println(sysdef.h);
        //System.out.print(output_fields_length);
        List<Tuple> tuples = new ArrayList<>();
        while (true) {
            Tuple t = rsIndexScan.get_next();
            if (t != null) {
                t.setHdr((short) inputFieldLength, schemaAttrTypes, null);
                Tuple tp = projectTuple(t, schemaAttrTypes, projectList);
                tuples.add(tp);
            } else break;
        }
        rsIndexScan.close();
        return tuples;
    }

    private List<Tuple> noIndex() throws Exception {


        AttrType[] schemaAttrTypes = getSchema(this.relName1);


        int input_fields_length = schemaAttrTypes.length;
        FldSpec[] outFldstack = new FldSpec[input_fields_length];
        for (int i = 0; i < input_fields_length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        //FileScan fs = new FileScan("test.heap", types, null, (short)1, (short)1, projlist, null);
        for (AttrType i : schemaAttrTypes) {
            System.out.println(i);
        }

        for (FldSpec i : outFldstack) {
            System.out.println(i);
        }
        FileScan scan = new FileScan(this.relName1, schemaAttrTypes, null, (short) input_fields_length, (short) input_fields_length, outFldstack, null);

//                    Sort sort = new Sort(schemaAttrTypes, (short)input_fields_length, null, scan, QA, new TupleOrder(TupleOrder.Ascending), 200, 2000, targetVect, k);
        int count = 0;
        List<Tuple> tuples = new ArrayList<>();
        Tuple result;
        while ((result = scan.get_next()) != null) {
            if (result.get100DVectFld(this.qa).distanceTo(this.t) <= d) {
                tuples.add(projectTuple(result, schemaAttrTypes, projectList));
            }

        }

        scan.close();
        return tuples;
    }

    private FileScan writeToTempHeapFile(List<Tuple> tuples) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException, InvalidTypeException, InvalidRelation, FileScanException, TupleUtilsException {
        Random rand = new Random();
        String heapFileName = this.relName1 + "_"+rand.nextInt(1000)+".heap";
        Heapfile tempHeapfile = new Heapfile(heapFileName);
        AttrType[] projAttrTypes = this.getProjectAttributeTypes(getSchema(this.relName1), this.projectList);
        for (Tuple tuple : tuples) {
            tuple.setHdr((short)(projAttrTypes.length), projAttrTypes, null);
            tempHeapfile.insertRecord(tuple.getTupleByteArray());
        }
        FldSpec[] outFldstack = new FldSpec[projAttrTypes.length];
        for (int i = 0; i < projAttrTypes.length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }
        return new FileScan(heapFileName,projAttrTypes,null, (short)projAttrTypes.length, projAttrTypes.length, outFldstack, null);
    }



    @Override
    public String getCommand() {
        return this.query;
    }



    @Override
    public void process() {
        try{
            parseQueryBody();
            List<Tuple> tuples;
            if (this.i.equalsIgnoreCase("H")){
                tuples = useIndex();
            }
            else {
                tuples = noIndex();
            }
            if (iter){
                this.iterator = writeToTempHeapFile(tuples);
            }
            else{
                for (Tuple tuple : tuples) {
                    printer(tuple, this.getProjectAttributeTypes());
                }
            }

        }
        catch (Exception e){
            printer("Error: NN scan on column "+this.qa +" with error "+e.getMessage());
            e.printStackTrace();
        }

    }

    public AttrType[] getProjectAttributeTypes() throws BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, IOException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {
        AttrType[] projAttrTypes = this.getProjectAttributeTypes(getSchema(this.relName1), this.projectList);
        return projAttrTypes;
    }
    public Iterator getIterator() {
        return this.iterator;
    }
    public int getProjQa(){
        return this.projQa;
    }
}
