package global;

/** 
 * Enumeration class for AttrOperator
 * 
 */

public class AttrOperator {

  public static final int aopEQ   = 0; // -> return true if the distance between operands distance param
  public static final int aopLT   = 1; // -> return true if distance between operands is lesser than distance param
  public static final int aopGT   = 2; // -> return true if distance between operands is greater than distance param
  public static final int aopNE   = 3; // -> return true if distance between operands is not equal to distance param
  public static final int aopLE   = 4; // -> return true if distance between operands is lesser than distance param
  public static final int aopGE   = 5;  //-> return true if distance between operands is greater or equal than distance param
  public static final int aopNOT  = 6; // -> not sure -> mostly will not apply since this is only for 1 operande
  public static final int aopNOP  = 7; // ->
  public static final int opRANGE = 8; //defined this way in C++

  public int attrOperator;

  /** 
   * AttrOperator Constructor
   * <br>
   * An attribute operator types can be defined as 
   * <ul>
   * <li>   AttrOperator attrOperator = new AttrOperator(AttrOperator.aopEQ);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (attrOperator.attrOperator == AttrOperator.aopEQ) ....
   * </ul>
   *
   * @param _attrOperator The available attribute operators 
   */

  public AttrOperator (int _attrOperator) {
    attrOperator = _attrOperator;
  }

  public String toString() {

    switch (attrOperator) {
    case aopEQ:
      return "aopEQ";
    case aopLT:
      return "aopLT";
    case aopGT:
      return "aopGT";
    case aopNE:
      return "aopNE";
    case aopLE:
      return "aopLE";
    case aopGE:
      return "aopGE";
    case aopNOT:
      return "aopNOT";
    case aopNOP:
      return "aopNOP";
    case opRANGE:
      return "opRANGE";
    }
    return ("Unexpected AttrOperator " + attrOperator);
  }
}
