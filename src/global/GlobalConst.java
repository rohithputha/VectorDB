package global;

public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 50;
   //changing number of buffers from 50 to 4000
  public static final int NUMBUF = 4000;

  /** Size of page. */
  //changing page size from 1024 to 4096
  public static final int MINIBASE_PAGESIZE = 4096;           // in bytes

  /** Size of each frame. */
   //changing buffer pool size from 1024 to 4096
  public static final int MINIBASE_BUFFER_POOL_SIZE = 4096;   // in Frames


  //changing space from 1024 to 4096
  public static final int MAX_SPACE = 4096;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 20000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    
  public static final int MAX_NAME = 50;

  public static final int INVALID_PAGE = -1;
}
