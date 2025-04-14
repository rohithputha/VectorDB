package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.*;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.RealKey;
import btree.StringKey;

/**
 * This file contains an implementation of the index nested loops join
 * algorithm. The algorithm is:
 *
 * foreach tuple r in R do
 *    use the index to find matching tuples s in S where join condition is satisfied
 *    for each matching tuple s
 *        add (r, s) to the result
 */

public class INLJoins extends Iterator {
  private AttrType[] _in1, _in2;
  private int in1_len, in2_len;
  private Iterator outer;
  private short t2_str_sizescopy[];
  private CondExpr OutputFilter[];
  private CondExpr RightFilter[];
  private int n_buf_pgs;        // # of buffer pages available.
  private boolean done;         // Is the join complete
  private Tuple outer_tuple, inner_tuple;
  private Tuple Jtuple;         // Joined tuple
  private FldSpec perm_mat[];
  private int nOutFlds;
  private String relationName;
  private IndexType indexType;
  private String indexName;
  private BTFileScan btScan;
  //private LSHFileScan lshScan;
  private AttrType[] Jtypes;
  private int joinAttrInOuter;  // The position of the join attribute in the outer relation
  private int joinAttrInInner;  // The position of the join attribute in the inner relation
  
  /**
   * Constructor
   * Initialize the two relations which are joined, including relation type,
   * @param in1  Array containing field types of R.
   * @param len_in1  # of columns in R.
   * @param t1_str_sizes shows the length of the string fields.
   * @param in2  Array containing field types of S
   * @param len_in2  # of columns in S
   * @param t2_str_sizes shows the length of the string fields.
   * @param amt_of_mem  IN PAGES
   * @param am1  access method for left i/p to join
   * @param relationName  access hfapfile for right i/p to join
   * @param index  the type of index used for join (B_Index or LSH)
   * @param indexName  the name of the index to be used
   * @param outFilter   select expressions
   * @param rightFilter reference to filter applied on right i/p
   * @param proj_list shows what input fields go where in the output tuple
   * @param n_out_flds number of outer relation fields
   * @exception IOException some I/O fault
   * @exception IndexException exception from index layers
   */
  public INLJoins(AttrType    in1[],    
                 int     len_in1,           
                 short   t1_str_sizes[],
                 AttrType    in2[],         
                 int     len_in2,           
                 short   t2_str_sizes[],   
                 int     amt_of_mem,        
                 Iterator     am1,          
                 String relationName,
                 IndexType indexType,
                 String indexName,      
                 CondExpr outFilter[],      
                 CondExpr rightFilter[],    
                 FldSpec   proj_list[],
                 int        n_out_flds
                ) throws IOException, IndexException, JoinsException
  {
    // Initialize arrays and variables
    _in1 = new AttrType[in1.length];
    _in2 = new AttrType[in2.length];
    System.arraycopy(in1, 0, _in1, 0, in1.length);
    System.arraycopy(in2, 0, _in2, 0, in2.length);
    in1_len = len_in1;
    in2_len = len_in2;
    
    outer = am1;
    t2_str_sizescopy = t2_str_sizes;
    inner_tuple = new Tuple();
    Jtuple = new Tuple();
    OutputFilter = outFilter;
    RightFilter = rightFilter;
    
    n_buf_pgs = amt_of_mem;
    this.relationName = relationName;
    this.indexType = indexType;
    this.indexName = indexName;
    done = false;
    
    perm_mat = proj_list;
    nOutFlds = n_out_flds;
    
    Jtypes = new AttrType[n_out_flds];
    
    try {
      // Set up the output tuple
      short[] t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                                          in1, len_in1, in2, len_in2,
                                          t1_str_sizes, t2_str_sizes,
                                          proj_list, n_out_flds);
    } catch (TupleUtilsException e) {
      throw new JoinsException(e, "TupleUtilsException is caught by INLJoins.java");
    }
    
    // Find the join attributes from the output filter
    try {
      identifyJoinAttributes();
    } catch (Exception e) {
      throw new JoinsException(e, "Failed to identify join attributes");
    }
    
    // Open the appropriate index scan based on index type
    try {
      if (indexType.toString() == "B_Index") {
        // Initialize BTree index scan
        BTreeFile btf = new BTreeFile(indexName);
        btScan = null; // Will be initialized when needed with the search key
      } else if (indexType.toString() == "LSHFIndex") {
        // Initialize LSH index scan for vector data
        //LSHFile lshf = new LSHFile(indexName);
        //lshScan = null; // Will be initialized when needed with the search key
        System.out.println("needs to be implemented");
      } else {
        throw new JoinsException("Invalid index type specified for INLJoins");
      }
    } catch (Exception e) {
      throw new JoinsException(e, "Failed to initialize index for INLJoins");
    }
  }
  
  /**
   * Identifies the join attributes from the OutputFilter condition expressions
   */
  private void identifyJoinAttributes() throws JoinsException {
    joinAttrInOuter = -1;
    joinAttrInInner = -1;
    
    if (OutputFilter == null) {
      throw new JoinsException("OutputFilter cannot be null for INLJoins");
    }

    // System.out.println(OutputFilter);
    // for(CondExpr i:OutputFilter){
    //     System.out.println(i.operand1.symbol.relation.key);
    // }
    
    // Find the equi-join condition in the OutputFilter
    for (int i = 0; OutputFilter != null && i < OutputFilter.length && OutputFilter[i] != null; i++) {
      if (OutputFilter[i].op.toString() == "aopEQ") {
        System.out.println("Entered if ");
        if (OutputFilter[i].type1.toString() == "attrSymbol" &&
            OutputFilter[i].type2.toString() == "attrSymbol") {
          // Check if this is a join condition between outer and inner relations
          System.out.println("Entered second if ");
          if (OutputFilter[i].operand1.symbol.relation.key == 0 && 
              OutputFilter[i].operand2.symbol.relation.key == 1) {
                System.out.println("entered thrid if");
            joinAttrInOuter = OutputFilter[i].operand1.symbol.offset;
            joinAttrInInner = OutputFilter[i].operand2.symbol.offset;
            break;
          }
        }
      }
    }
    
    if (joinAttrInOuter == -1 || joinAttrInInner == -1) {
      // If no equi-join found, try to find other distance-based conditions for vectors
      for (int i = 0; OutputFilter != null && i < OutputFilter.length && OutputFilter[i] != null; i++) {
        if (OutputFilter[i].op.attrOperator == AttrOperator.aopLE ||
            OutputFilter[i].op.attrOperator == AttrOperator.aopLT) {
          // This could be a distance-based condition for vectors
          if (OutputFilter[i].type1.attrType == AttrType.attrSymbol &&
              OutputFilter[i].type2.attrType == AttrType.attrSymbol) {
            joinAttrInOuter = OutputFilter[i].operand1.symbol.offset;
            joinAttrInInner = OutputFilter[i].operand2.symbol.offset;
            break;
          }
        }
      }
    }
    
    if (joinAttrInOuter == -1 || joinAttrInInner == -1) {
      throw new JoinsException("No suitable join condition found in OutputFilter");
    }
  }
  
  /**
   * @return The joined tuple is returned
   * @exception IOException I/O errors
   * @exception JoinsException some join exception
   * @exception IndexException exception from index
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception InvalidTypeException tuple type not valid
   * @exception PageNotReadException exception from lower layer
   * @exception TupleUtilsException exception from using tuple utilities
   * @exception PredEvalException exception from PredEval class
   * @exception SortException sort exception
   * @exception LowMemException memory error
   * @exception UnknowAttrType attribute type unknown
   * @exception UnknownKeyTypeException key type unknown
   * @exception Exception other exceptions
   */
  public Tuple get_next() 
    throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
           InvalidTypeException, PageNotReadException, TupleUtilsException, 
           PredEvalException, SortException, LowMemException, UnknowAttrType,
           UnknownKeyTypeException, Exception
  {
    if (done) {
      return null;
    }
    
    // Loop until we find a matching pair or run out of outer tuples
    do {
      // Get next outer tuple if needed
      if ((outer_tuple = outer.get_next()) == null) {
        done = true;
        closeIndexScan();
        return null;
      }
      
      // Extract the join attribute value from the outer tuple
      KeyClass key = extractKeyFromOuterTuple();
      if (key == null) {
        continue; // Skip this outer tuple if we can't extract a valid key
      }
      
      // Start a new index scan with the extracted key
      initializeIndexScan(key);
      
      // Find matching inner tuples using the index
      RID rid = new RID();
      if (indexType.toString() == "B_Index") {
        KeyDataEntry entry;
        while ((entry = btScan.get_next()) != null) {
          rid = ((LeafData) entry.data).getData();
          getInnerTupleFromRID(rid);
          
          // Apply right filter if exists
          if (RightFilter != null && !PredEval.Eval(RightFilter, inner_tuple, null, _in2, null)) {
            continue;
          }
          
          // Apply output filter to check join condition
          if (OutputFilter != null && PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2)) {
            // We found a matching pair, perform projection and return the joined tuple
            Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
            return Jtuple;
          }
        }
      } else if (indexType.toString() == "LSHFIndex") {
        // KeyDataEntry entry;
        // while ((entry = lshScan.get_next()) != null) {
        //   rid = entry.data;
        //   getInnerTupleFromRID(rid);
          
        //   // Apply right filter if exists
        //   if (RightFilter != null && !PredEval.Eval(RightFilter, inner_tuple, null, _in2, null)) {
        //     continue;
        //   }
          
        //   // Apply output filter to check join condition
        //   if (OutputFilter != null && PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2)) {
        //     // We found a matching pair, perform projection and return the joined tuple
        //     Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
        //     return Jtuple;
        //   }
        // }
        System.out.println("needs to be implemented");
      }
      
      // Close current index scan when done with this outer tuple
      closeIndexScan();
      
    } while (true);
  }
  
  /**
   * Extract the join key from the outer tuple based on the join attribute
   */
  private KeyClass extractKeyFromOuterTuple() 
    throws IOException, UnknowAttrType, JoinsException
  {
    try {
      if (_in1[joinAttrInOuter - 1].attrType == AttrType.attrInteger) {
        int key = outer_tuple.getIntFld(joinAttrInOuter);
        return new IntegerKey(key);
      } else if (_in1[joinAttrInOuter - 1].attrType == AttrType.attrReal) {
        float key = outer_tuple.getFloFld(joinAttrInOuter);
        return new RealKey(key);
      } else if (_in1[joinAttrInOuter - 1].attrType == AttrType.attrString) {
        String key = outer_tuple.getStrFld(joinAttrInOuter);
        return new StringKey(key);
      } 
    //   else if (_in1[joinAttrInOuter - 1].attrType == AttrType.attrVector100D) {
    //     // For vector attributes using LSH index
    //     Vector100Dtype key = outer_tuple.get100DVectFld(joinAttrInOuter);
    //     return new VectorKey(key);
    //   }
       else {
        throw new UnknowAttrType("Unknown attribute type in extractKeyFromOuterTuple");
      }
    } catch (Exception e) {
      throw new JoinsException(e, "Error extracting key from outer tuple");
    }
  }
  
  /**
   * Initialize the appropriate index scan based on the key from outer tuple
   */
  private void initializeIndexScan(KeyClass key) 
    throws IndexException, IOException, JoinsException
  {
    try {
      if (indexType.toString() == "B_Index") {
        // Close previous scan if it exists
        if (btScan != null) {
          btScan.DestroyBTreeFileScan();
        }
        
        // Open new BTree scan with the key
        BTreeFile btf = new BTreeFile(indexName);
        btScan = btf.new_scan(key, key);
      } 
    //   else if (indexType == IndexType.LSH) {
    //     // Close previous scan if it exists
    //     if (lshScan != null) {
    //       lshScan.close();
    //     }
        
    //     // For LSH, we need to handle vector key differently
    //     LSHFile lshf = new LSHFile(indexName);
    //     if (key instanceof VectorKey) {
    //       VectorKey vKey = (VectorKey) key;
    //       lshScan = lshf.new_scan(vKey);
    //     } else {
    //       throw new JoinsException("Expected VectorKey for LSH index scan");
    //     }
    //   }
    } catch (Exception e) {
      throw new JoinsException(e, "Error initializing index scan");
    }
  }
  
  /**
   * Close the current index scan
   */
  private void closeIndexScan() 
    throws JoinsException, IOException
  {
    try {
      if (indexType.toString() == "B_Index" && btScan != null) {
        btScan.DestroyBTreeFileScan();
        btScan = null;
      } 
    //   else if (indexType == IndexType.LSH && lshScan != null) {
    //     lshScan.close();
    //     lshScan = null;
    //   }
    } catch (Exception e) {
      throw new JoinsException(e, "Error closing index scan");
    }
  }
  
  /**
   * Get the inner tuple given the RID
   */
  private void getInnerTupleFromRID(RID rid) 
    throws JoinsException, IOException
  {
    try {
      Heapfile hf = new Heapfile(relationName);
      inner_tuple = hf.getRecord(rid);
      inner_tuple.setHdr((short)in2_len, _in2, t2_str_sizescopy);
    } catch (Exception e) {
      throw new JoinsException(e, "Error getting inner tuple from RID");
    }
  }
  
  /**
   * Implement the abstract method close() from super class Iterator
   * to finish cleaning up
   * @exception IOException I/O error from lower layers
   * @exception JoinsException join error from lower layers
   * @exception IndexException index access error 
   */
  public void close() 
    throws JoinsException, IOException, IndexException
  {
    if (!closeFlag) {
      try {
        outer.close();
        closeIndexScan();
      } catch (Exception e) {
        throw new JoinsException(e, "INLJoins.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}