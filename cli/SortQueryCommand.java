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

public class SortQueryCommand implements VectorDbCommand {

    private final String query;
    private final String QUERY = "Sort";
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
    public SortQueryCommand(String relName1, String relName2, String query) {
        this.query = query;
        this.relName1 = relName1;
        this.relName2 = relName2;
    }
    private Vector100Dtype targetVect;

    private void parseQueryBody() throws ParseException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, InvalidPageNumberException, PagePinnedException, InvalidFrameNumberException, IOException, FileIOException, PageUnpinnedException, DiskMgrException, ReplacerException {

        String queryBody = this.query.split("[()]")[1];
        String[] args = queryBody.split(",");
        this.qa = Integer.parseInt(args[0].trim());

        System.out.println(this.qa);
        this.qaAttributeType = getSchema(this.relName1)[this.qa-1];
        System.out.println(qaAttributeType);

        this.t = args[1].trim();
        BufferedReader bufr = new BufferedReader(new FileReader(this.t + ".txt"));
        String vec = bufr.readLine();
        bufr.close();
        String[] veclist = vec.trim().split("\\s+");
        this.targetVect = new Vector100Dtype();
        for (int i = 0; i < 100; i++) {
            targetVect.set(i, Short.parseShort(veclist[i]));
        }

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
        projectList = new ArrayList<>();
        for (int i = 3; i < args.length; i++) {
            projectList.add(Integer.parseInt(args[i].trim()));
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

        

        // FileScan fs = new FileScan(this.relName1, attrTypes, null, (short) (attrTypes.length), (short) (attrTypes.length), projectList, null);
        FileScan fs = new FileScan(this.relName1, attrTypes, null, (short) (attrTypes.length), (short) (attrTypes.length), outFldstack, null);
        
        Sort sort = new Sort(attrTypes, (short) (attrTypes.length), null, fs, qa, new TupleOrder(TupleOrder.Ascending), 200, 2000, this.targetVect, k);
      
        
        int count = 0;
        Tuple result;

        List<Tuple> tuples = new ArrayList<>();
        // while ((result = sort.get_next()) != null && count < this.k) {
        while ((result = sort.get_next()) != null) {
            tuples.add(projectTuple(result, attrTypes, projectList));

            for (int i = 1; i <= attrTypes.length; i++) {
                AttrType fieldType = attrTypes[i - 1];
                System.out.print("Field " + i + ": ");
                
                switch (fieldType.attrType) {
                    case AttrType.attrInteger:
                        int intValue = result.getIntFld(i);
                        System.out.println(intValue);
                        break;
                    case AttrType.attrReal:
                        float floatValue = result.getFloFld(i);
                        System.out.println(floatValue);
                        break;
                    case AttrType.attrString:
                        String strValue = result.getStrFld(i);
                        System.out.println(strValue);
                        break;
                    case AttrType.attrVector100D:
                        Vector100Dtype vectorValue = result.get100DVectFld(i);
                        short[] vector = vectorValue.getVector();
                        System.out.println(Arrays.toString(vector));
                        break;
                    default:
                        System.out.println("Unknown type");
                        break;
                }
            }
            System.out.println("---"); // Separator between tuples


            count++;
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
            List<Tuple> t = fileScanHeapFile();
            this.results = t;
            this.projectAttrSchema = getProjectAttributeTypes(getSchema(this.relName1), this.projectList);
        }
        catch(Exception e) {
            printer("Error: error while sorting. " + e.getMessage());
        }
    }

    public List<Tuple> results;
    public AttrType[] projectAttrSchema;

}
