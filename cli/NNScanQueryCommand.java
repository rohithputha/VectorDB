package cli;

import LSHFIndex.LSHFIndexFile;
import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.*;
import index.NNIndexScan;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NNScanQueryCommand implements VectorDbCommand{
    private final String query;
    private final String COMMAND = "NN";
    private String relName1;
    private String relName2;
    private String queryBody;
    public NNScanQueryCommand(String relName1, String relName2, String queryHead, String queryBody) {
        this.query = queryHead;
        this.relName1 = relName1;
        this.relName2 = relName2;
        this.queryBody = queryBody;
    }

    private int qa;
    private String targetFile;
    private Vector100Dtype t;
    private int k;
    private String i;
    private String j;
    private List<Integer> projectList;
    private AttrType qaAttributeType;
    private LSHFIndexFile lshfIndexFile;
    private void parseQueryBody() throws BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, IOException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {
        String[] args = this.queryBody.split(",");
        this.qa = Integer.parseInt(args[0]);
        this.qaAttributeType = getSchema(this.relName1)[this.qa];
        this.targetFile = args[1];

        ///  Write logic to read the target file...

        this.k = Integer.parseInt(args[2]);
        this.i = args[3];

        if (this.i.equalsIgnoreCase("H")) {
            try {
               lshfIndexFile = new LSHFIndexFile(this.relName1 ); /// need to find the h and l values for index name;
            } catch (Exception e) {
                lshfIndexFile = null;
            }
        }
        projectList = new ArrayList<>();
        for (int i = 4; i < args.length; i++) {
            projectList.add(Integer.parseInt(args[i]));
        }

    }



    private AttrType[] getProjectAttributeTypes(AttrType[] originalTypes, List<Integer> projectList) {
        AttrType[] projAttrTypes = new AttrType[projectList.size()];
        for (int i = 0; i < projectList.size(); i++) {
            projAttrTypes[i] = originalTypes[projectList.get(i)-1];
            /// CHECK: 0 base or 1 based indexing
        }
        return projAttrTypes;
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
        PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL_" + this.relName1+"_"+this.qa);
        HFPage page = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pid, page, false);
        Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
        m.setHdr((short) 3,
                new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                null);
        int h = m.getIntFld(1);
        int L = m.getIntFld(2);
        int input_fields_length = m.getIntFld(3);


        AttrType[] schemaAttrTypes = getSchema(this.relName1);

        SystemDefs.JavabaseBM.unpinPage(pid, false);
        String index_name = this.relName1 + Integer.toString(this.qa) + "_" + Integer.toString(h) + "_" + Integer.toString(L);
//                    String index_name = dbName + Integer.toString(QA-1)  + Integer.toString(h)  + Integer.toString(L);


        int output_fields_length = input_fields_length;

        FldSpec[] outFldstack = new FldSpec[output_fields_length];
        for (int i = 0; i < output_fields_length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }


        NNIndexScan nnIndexScan = new NNIndexScan(
                new IndexType(IndexType.LSHFIndex),
                this.relName1,
                index_name,
                schemaAttrTypes,
                null,
                input_fields_length,
                output_fields_length,
                outFldstack,
                null,
                this.qa,
                this.t,
                this.k);
        // System.out.println(sysdef.h);
        //System.out.print(output_fields_length);
        List<Tuple> tuples = new ArrayList<>();
        while (true) {
            Tuple t = nnIndexScan.get_next();
            if (t != null) {
                t.setHdr((short) input_fields_length, schemaAttrTypes, null);
                Tuple tp = projectTuple(t, schemaAttrTypes, projectList);
                tuples.add(tp);
            } else break;
        }
        nnIndexScan.close();
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
        FileScan scan = new FileScan(this.relName1, schemaAttrTypes, null, (short) schemaAttrTypes.length, (short) schemaAttrTypes.length, outFldstack, null);

        Sort sort = new Sort(schemaAttrTypes, (short) input_fields_length, null, scan, this.qa, new TupleOrder(TupleOrder.Ascending), 200, 2000, this.t, k);
        int count = 0;
        Tuple result;

        List<Tuple> tuples = new ArrayList<>();
        while ((result = sort.get_next()) != null && count < this.k) {
            tuples.add(projectTuple(result, schemaAttrTypes, projectList));
//            printer(params, schemaAttrTypes, result);
            count++;
        }
        scan.close();
        sort.close();
        // //System.out.println(scan.getNext());
        // scan.closescan();
        return tuples;
    }


    @Override
    public String getCommand() {
        return this.query;
    }

    @Override
    public void process() {
        try{
            List<Tuple> tuples;
            if (this.i.equalsIgnoreCase("H")){
                tuples = useIndex();
            }
            else {
                tuples = noIndex();
            }
            /// print the tuples
        }
        catch (Exception e){
           printer("Error: NN scan on column "+this.qa +" with error "+e.getMessage());
        }

    }
}
