package cli;

import btree.*;
import bufmgr.*;
import com.sun.jdi.ObjectReference;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.AttrType;
import global.RID;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FilterQueryCommand implements VectorDbCommand {

    private final String query;
    private final String QUERY = "filter";
    private String relName1;
    private String relName2;



    private int qa;
    private String t;
    private int k;
    private String i;
    private BTreeFile bTreeFile;
    private List<Integer> projectList;
    private KeyClass keyClass;
    private AttrType qaAttributeType;
    public FilterQueryCommand(String relName1, String relName2, String query) {
        this.query = query;
        this.relName1 = relName1;
        this.relName2 = relName2;
    }

    private void parseQueryBody() throws ParseException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, IOException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {

        String queryBody = this.query.split("[()]")[1];
        String[] args = queryBody.split(",");
        this.qa = Integer.parseInt(args[0].trim());
        this.qaAttributeType = getSchema(this.relName1)[this.qa-1];
        this.t = args[1].trim();
        this.k = Integer.parseInt(args[2].trim());
        this.i = args[3].trim();
        switch (qaAttributeType.attrType) {
            case AttrType.attrInteger:
                this.keyClass = new IntegerKey(Integer.parseInt(t));
                break;
            case AttrType.attrString:
                this.keyClass = new StringKey(t);
                break;
            case AttrType.attrReal:
                this.keyClass = new RealKey(Float.parseFloat(t));
                break;
            default:
                throw new ParseException("Unknown attribute type", 0);
        }
        if (this.i.equalsIgnoreCase("Y")) {
            try {
                bTreeFile = new BTreeFile(this.relName1 + "_" + this.qa);
            } catch (Exception e) {
                bTreeFile = null;
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

    private List<KeyDataEntry> useIndexToFilter() throws IteratorException, ConstructPageException, KeyNotMatchException, PinPageException, IOException, UnpinPageException, ScanIteratorException {
        if (bTreeFile == null) {
            return null;
        }
        List<KeyDataEntry> results = new ArrayList<KeyDataEntry>();
        BTFileScan btFileScan = bTreeFile.new_scan(this.keyClass, this.keyClass);
        while (true) {
            KeyDataEntry keyDataEntry = btFileScan.get_next();
            if (keyDataEntry == null) {
                break;
            }
            results.add(keyDataEntry);
        }
        return results;
    }

    private boolean filter(Tuple temp) throws FieldNumberOutOfBoundException, IOException {
        if (qaAttributeType.attrType == AttrType.attrInteger) {
            return temp.getIntFld(this.qa) == Integer.parseInt(this.t);
        }
        else if (qaAttributeType.attrType == AttrType.attrString) {
            return temp.getStrFld(this.qa) .equals(this.t);
        }
        else if (qaAttributeType.attrType == AttrType.attrReal) {
            return temp.getFloFld(this.qa) == Float.parseFloat(this.t);
        }
        return false;
    }

    private List<Tuple> fileScanHeapFile(List<Integer> projectAttr) throws InvalidRelation, FileScanException, IOException, TupleUtilsException, PageNotReadException, UnknowAttrType, FieldNumberOutOfBoundException, PredEvalException, WrongPermat, InvalidTupleSizeException, JoinsException, InvalidTypeException, BufferPoolExceededException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {
        AttrType[] attrTypes = getSchema(this.relName1);
        FldSpec[] projectList = new FldSpec[attrTypes.length];
        for (int i = 0; i < attrTypes.length; i++) {
            projectList[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            /// CHECK : check if attributes are 1 or 0 based indexing.
        }

        FileScan fs = new FileScan(this.relName1, attrTypes, null, (short) (attrTypes.length), (short) (attrTypes.length), projectList, null);
        List<Tuple> tupleList = new ArrayList<>();
        while (true) {
            Tuple temp = fs.get_next();
            if (temp == null) {
                break;
            }
            temp.setHdr((short) attrTypes.length, attrTypes, null);
            if (this.filter(temp)) {
                tupleList.add(this.projectTuple(temp,attrTypes, this.projectList));
            }

        }
        return tupleList;
    }

    private List<Tuple> getHeapFileRecords(List<Integer> projectList, List<KeyDataEntry> keyDataEntries) throws Exception {
        if (keyDataEntries == null) {
            return fileScanHeapFile(projectList);
        }
        Heapfile heapfile = new Heapfile(this.relName1);
        List<Tuple> tupleList = new ArrayList<>();
        AttrType[] attrTypes = getSchema(this.relName1);

        for (KeyDataEntry keyDataEntry : keyDataEntries) {
            LeafData leafData = (LeafData) keyDataEntry.data;
            Tuple temp = heapfile.getRecord(new RID(leafData.getData().pageNo, leafData.getData().slotNo));
            temp.setHdr((short) attrTypes.length, attrTypes, null);
            tupleList.add(this.projectTuple(temp, attrTypes, projectList));
        }
        return tupleList;
    }

    @Override
    public String getCommand() {
        return this.query;
    }

    @Override
    public void process() {
        try{
            ///  Add logic to return only the first k elements...

            parseQueryBody();
            List<Tuple> t = this.getHeapFileRecords(this.projectList, useIndexToFilter());
            this.results = t;
            this.projectAttrSchema = getProjectAttributeTypes(getSchema(this.relName1), this.projectList);
            for (Tuple tuple : t) {
                printer(tuple, this.projectAttrSchema);
            }
        }
        catch(Exception e) {
            printer("Error: error while filtering. " + e.getMessage());
        }
    }

    public List<Tuple> results;
    public AttrType[] projectAttrSchema;
}
