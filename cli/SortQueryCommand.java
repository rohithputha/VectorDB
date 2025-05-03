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
import global.TupleOrder;
import global.Vector100Dtype;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Random;

public class SortQueryCommand implements VectorDbCommand {

    private final String query;
    private final String QUERY = "Sort";
    private String relName1;
    private String relName2;
    private int allocatedBuff;



    private int qa;
    // private String t;
    private int k;
    private String i;
    private BTreeFile bTreeFile;
    private List<Integer> projectList;
    private KeyClass keyClass;
    private AttrType qaAttributeType;
    private Iterator tupleIterator;
    public SortQueryCommand(String relName1, String relName2, String query, int allocatedBuff) {
        this.query = query;
        this.relName1 = relName1;
        this.relName2 = relName2;
        this.allocatedBuff = allocatedBuff;
    }
    private String targetFile;
    private Vector100Dtype t;
    // private Vector100Dtype targetVect;

    private void parseQueryBody() throws Exception {

        String queryBody = this.query.split("[()]")[1];
        String[] args = queryBody.split(",");
        this.qa = Integer.parseInt(args[0].trim());

        // System.out.println(this.qa);
        this.qaAttributeType = getSchema(this.relName1)[this.qa-1];
        this.targetFile = args[1].trim();
        this.t = getTaretVector(this.targetFile);

        System.out.println(qaAttributeType);

        // this.t = args[1].trim();
        // BufferedReader bufr = new BufferedReader(new FileReader(this.t + ".txt"));
        // String vec = bufr.readLine();
        // bufr.close();
        // String[] veclist = vec.trim().split("\\s+");
        // this.targetVect = new Vector100Dtype();
        // for (int i = 0; i < 100; i++) {
        //     targetVect.set(i, Short.parseShort(veclist[i]));
        // }

        this.k = Integer.parseInt(args[2].trim());
        // switch (qaAttributeType.attrType) {
        //     case AttrType.attrInteger:
        //         this.keyClass = new IntegerKey(Integer.parseInt(t));
        //         break;
        //     case AttrType.attrString:
        //         this.keyClass = new StringKey(t);
        //         break;
        //     case AttrType.attrReal:
        //         this.keyClass = new IntegerKey(Integer.parseInt(t));
        //         break;
        //     default:
        //         throw new ParseException("Unknown attribute type", 0);
        // }
        // projectList = new ArrayList<>();
        // for (int i = 3; i < args.length; i++) {
        //     projectList.add(Integer.parseInt(args[i].trim()));
        // }

        projectList = new ArrayList<>();
        System.out.println(args[3]);
        if(args[3].trim().equals("*")){
            AttrType[] at1 = getSchema(this.relName1);
            for(int i=0;i<at1.length;i++){
                projectList.add(i+1);
            }
        }
        else{
            for (int i = 3; i < args.length; i++) {
                projectList.add(Integer.parseInt(args[i].trim()));
            } 
        }

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


    private List<Tuple> fileScanHeapFile() throws InvalidRelation, FileScanException, IOException, TupleUtilsException, PageNotReadException, UnknowAttrType, FieldNumberOutOfBoundException, PredEvalException, WrongPermat, InvalidTupleSizeException, JoinsException, InvalidTypeException, BufferPoolExceededException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException, SortException, LowMemException, Exception {
        AttrType[] attrTypes = getSchema(this.relName1);

        // FldSpec[] projectList = new FldSpec[attrTypes.length];
        // for (int i = 0; i < attrTypes.length; i++) {
        //     projectList[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        // }

        int input_fields_length = attrTypes.length;
        FldSpec[] outFldstack = new FldSpec[input_fields_length];
        for (int i = 0; i < input_fields_length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }

        
        FileScan fs = new FileScan(this.relName1, attrTypes, null, (short) (attrTypes.length), (short) (attrTypes.length), outFldstack, null);
        Sort sort = new Sort(attrTypes, (short) (attrTypes.length), null, fs, qa, new TupleOrder(TupleOrder.Ascending), 200, (int)(this.allocatedBuff * 0.9), this.t, k);
      
        Tuple result;
        int count = 0;
        List<Tuple> tuples = new ArrayList<>();

        if (k == 0) {
            while ((result = sort.get_next()) != null) {
                tuples.add(projectTuple(result, attrTypes, projectList));
                count ++;
            }
        } else {
            while ((result = sort.get_next()) != null && count < this.k) {
                tuples.add(projectTuple(result, attrTypes, projectList));
                count ++;
            }
        }
        
        fs.close();
        sort.close();
        return tuples;
    }


    @Override
    public String getCommand() {
        return this.query;
    }

    @Override
    public void process() {
        try{
            parseQueryBody();
            List<Tuple> tuples = fileScanHeapFile();

            // this.results = t;
            // this.projectAttrSchema = getProjectAttributeTypes(getSchema(this.relName1), this.projectList);

            for (Tuple tuple : tuples) {
                printer(tuple, this.getProjectAttributeTypes());

            }
        }
        catch(Exception e) {
            printer("Error: error while sorting. " + e.getMessage());
            e.printStackTrace();
        }
    }


    public AttrType[] getProjectAttributeTypes() throws BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, IOException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {
        AttrType[] projAttrTypes = this.getProjectAttributeTypes(getSchema(this.relName1), this.projectList);
        return projAttrTypes;
    }
    public Iterator getIterator() {
        return this.tupleIterator;
    }
    public int getProjQa(){
        return this.projQa;
    }

}
