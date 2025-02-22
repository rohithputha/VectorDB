package iterator;

import heap.*;
import global.*;
import java.io.*;

public class PredEval {
    /**
     * Predicate evaluate, according to the condition ConExpr, judge if 
     * the two tuples can join. If so, return true, otherwise false.
     *
     * @param p[] single select condition array
     * @param t1 compared tuple1
     * @param t2 compared tuple2
     * @param in1[] the attribute types corresponding to t1
     * @param in2[] the attribute types corresponding to t2
     * @return true if condition holds, false otherwise
     * @exception IOException some I/O error
     * @exception UnknowAttrType unknown attribute type
     * @exception InvalidTupleSizeException size of tuple not valid
     * @exception InvalidTypeException type of tuple not valid
     * @exception FieldNumberOutOfBoundException field number exceeds limit
     * @exception PredEvalException exception from this method
     */
    public static boolean Eval(CondExpr p[], Tuple t1, Tuple t2, AttrType in1[], AttrType in2[])
            throws IOException, UnknowAttrType, InvalidTupleSizeException, InvalidTypeException,
                   FieldNumberOutOfBoundException, PredEvalException {
        CondExpr temp_ptr;
        int i = 0;
        Tuple tuple1 = null, tuple2 = null;
        int fld1, fld2;
        Tuple value = new Tuple();
        short[] str_size = new short[1];
        AttrType[] val_type = new AttrType[1];

        int comp_res;
        boolean op_res = false, row_res = false, col_res = true;

        if (p == null) {
            return true;
        }

        while (p[i] != null) {
            temp_ptr = p[i];
            while (temp_ptr != null) {
                val_type[0] = new AttrType(temp_ptr.type1.attrType);
                fld1 = 1;

                // Setup first operand
                switch (temp_ptr.type1.attrType) {
                    case AttrType.attrInteger:
                        value.setHdr((short) 1, val_type, null);
                        value.setIntFld(1, temp_ptr.operand1.integer);
                        tuple1 = value;
                        break;
                    case AttrType.attrReal:
                        value.setHdr((short) 1, val_type, null);
                        value.setFloFld(1, temp_ptr.operand1.real);
                        tuple1 = value;
                        break;
                    case AttrType.attrString:
                        str_size[0] = (short) (temp_ptr.operand1.string.length() + 1);
                        value.setHdr((short) 1, val_type, str_size);
                        value.setStrFld(1, temp_ptr.operand1.string);
                        tuple1 = value;
                        break;
                    case AttrType.attrVector100D:
                        value.setHdr((short) 1, val_type, null);
                        value.set100DVectFld(1, temp_ptr.operand1.vector);
                        tuple1 = value;
                        break;
                    case AttrType.attrSymbol:
                        fld1 = temp_ptr.operand1.symbol.offset;
                        if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
                            tuple1 = t1;
                        } else {
                            tuple1 = t2;
                        }
                        break;
                    default:
                        throw new UnknowAttrType("Unknown attribute type: " + temp_ptr.type1.attrType);
                }

                // Setup second operand
                val_type[0] = new AttrType(temp_ptr.type2.attrType);
                fld2 = 1;
                switch (temp_ptr.type2.attrType) {
                    case AttrType.attrInteger:
                        value.setHdr((short) 1, val_type, null);
                        value.setIntFld(1, temp_ptr.operand2.integer);
                        tuple2 = value;
                        break;
                    case AttrType.attrReal:
                        value.setHdr((short) 1, val_type, null);
                        value.setFloFld(1, temp_ptr.operand2.real);
                        tuple2 = value;
                        break;
                    case AttrType.attrString:
                        str_size[0] = (short) (temp_ptr.operand2.string.length() + 1);
                        value.setHdr((short) 1, val_type, str_size);
                        value.setStrFld(1, temp_ptr.operand2.string);
                        tuple2 = value;
                        break;
                    case AttrType.attrVector100D:
                        value.setHdr((short) 1, val_type, null);
                        value.set100DVectFld(1, temp_ptr.operand2.vector);
                        tuple2 = value;
                        break;
                    case AttrType.attrSymbol:
                        fld2 = temp_ptr.operand2.symbol.offset;
                        if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer) {
                            tuple2 = t1;
                        } else {
                            tuple2 = t2;
                        }
                        break;
                    default:
                        throw new UnknowAttrType("Unknown attribute type: " + temp_ptr.type2.attrType);
                }

                // Perform comparison
                try {
                    if (temp_ptr.type1.attrType == AttrType.attrVector100D || 
                        temp_ptr.type2.attrType == AttrType.attrVector100D) {
                        // Handle Vector100Dtype comparisons
                        if (tuple1 == t1 && tuple2 == t2) {
                            // Tuple vs. Tuple
                            comp_res = t1.compareTupleWithTuple(fld1, t2, fld2, in1);
                        } else if (tuple1 == t1 && tuple2 != t2) {
                            // Tuple vs. Value
                            comp_res = t1.compareTupleWithValue(fld1, tuple2.get100DVectFld(1), 
                                                               new AttrType(AttrType.attrVector100D));
                        } else if (tuple1 != t1 && tuple2 == t2) {
                            // Value vs. Tuple
                            comp_res = t2.compareTupleWithValue(fld2, tuple1.get100DVectFld(1), 
                                                               new AttrType(AttrType.attrVector100D));
                        } else {
                            // Value vs. Value (shouldn't happen in predicate eval, but handle for completeness)
                            Vector100Dtype v1 = tuple1.get100DVectFld(1);
                            Vector100Dtype v2 = tuple2.get100DVectFld(1);
                            comp_res = v1.distanceTo(v2);
                        }
                    } else {
                        // Non-Vector100Dtype comparisons
                        comp_res = TupleUtils.CompareTupleWithTuple(
                            new AttrType(temp_ptr.type1.attrType), tuple1, fld1, tuple2, fld2);
                    }
                } catch (TupleUtilsException e) {
                    throw new PredEvalException(e, "TupleUtilsException caught in PredEval.java");
                }

                // Evaluate operator
                op_res = false;
                if (temp_ptr.type1.attrType == AttrType.attrVector100D || 
                    temp_ptr.type2.attrType == AttrType.attrVector100D) {
                    // Vector100Dtype uses distance-based comparison
                    switch (temp_ptr.op.attrOperator) {
                        case AttrOperator.aopEQ:
                            op_res = (comp_res == temp_ptr.distance);
                            break;
                        case AttrOperator.aopLT:
                            op_res = (comp_res < temp_ptr.distance);
                            break;
                        case AttrOperator.aopGT:
                            op_res = (comp_res > temp_ptr.distance);
                            break;
                        case AttrOperator.aopNE:
                            op_res = (comp_res != temp_ptr.distance);
                            break;
                        case AttrOperator.aopLE:
                            op_res = (comp_res <= temp_ptr.distance);
                            break;
                        case AttrOperator.aopGE:
                            op_res = (comp_res >= temp_ptr.distance);
                            break;
                        default:
                            throw new PredEvalException("Unsupported operator for Vector100Dtype: " + 
                                                        temp_ptr.op.attrOperator);
                    }
                } else {
                    // Non-Vector100Dtype uses standard comparison
                    switch (temp_ptr.op.attrOperator) {
                        case AttrOperator.aopEQ:
                            if (comp_res == 0) op_res = true;
                            break;
                        case AttrOperator.aopLT:
                            if (comp_res < 0) op_res = true;
                            break;
                        case AttrOperator.aopGT:
                            if (comp_res > 0) op_res = true;
                            break;
                        case AttrOperator.aopNE:
                            if (comp_res != 0) op_res = true;
                            break;
                        case AttrOperator.aopLE:
                            if (comp_res <= 0) op_res = true;
                            break;
                        case AttrOperator.aopGE:
                            if (comp_res >= 0) op_res = true;
                            break;
                        case AttrOperator.aopNOT:
                            if (comp_res != 0) op_res = true;
                            break;
                        default:
                            break;
                    }
                }

                row_res = row_res || op_res;
                if (row_res) {
                    break; // OR predicates satisfied
                }
                temp_ptr = temp_ptr.next;
            }
            i++;
            col_res = col_res && row_res;
            if (!col_res) {
                return false;
            }
            row_res = false; // Starting next row
        }

        return true;
    }
}