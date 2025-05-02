package LSHFIndex;

import btree.KeyClass;
import global.Vector100Dtype;

/**  StringKey: It extends the KeyClass.
 *   It defines the string Key.
 */
public class VectKey extends KeyClass {

    private Vector100Dtype key;

//    public String toString(){
//        return key;
//    }

    /** Class constructor
     *  @param     s   the value of the string key to be set
     */
    public VectKey(Vector100Dtype s) { key = s; }

    /** get a copy of the istring key
     *  @return the reference of the copy
     */
    public Vector100Dtype getKey() {return key;}

    /** set the string key value
     */
    public void setKey(Vector100Dtype s) { key=s;}
}
