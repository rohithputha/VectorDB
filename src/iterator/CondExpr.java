package iterator;

import java.io.*;
import global.*;

/**
 * This class will hold single select condition
 * It is an element of linked list which is logically
 * connected by OR operators.
 */
public class CondExpr {
  
    /**
     * Operator like "<"
     */
    public AttrOperator op;    
  
    /**
     * Types of operands, Null AttrType means that operand is not a
     * literal but an attribute name
     */    
    public AttrType type1;
    public AttrType type2;    
 
    /**
     * The left operand and right operand 
     */ 
    public Operand operand1;
    public Operand operand2;
  
    /**
     * Distance threshold for attrVector100D comparisons
     */
    public int distance;  // New field for Task 2.3
  
    /**
     * Pointer to the next element in linked list
     */    
    public CondExpr next;   
  
    /**
     * Constructor
     */
    public CondExpr() {
        operand1 = new Operand();
        operand2 = new Operand();
        
        operand1.integer = 0;
        operand2.integer = 0;
        
        op = new AttrOperator(AttrOperator.aopEQ); // Default operator
        type1 = new AttrType(AttrType.attrNull);   // Default type
        type2 = new AttrType(AttrType.attrNull);   // Default type
        distance = 0;                              // Default distance
        next = null;
    }

    /**
     * Set the distance threshold (must be non-negative)
     */
    public void setDistance(int dist) {
        if (dist < 0) {
            throw new IllegalArgumentException("Distance must be non-negative for attrVector100D");
        }
        this.distance = dist;
    }
}