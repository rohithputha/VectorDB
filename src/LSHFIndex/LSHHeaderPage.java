package LSHFIndex;

import btree.ConstructPageException;
import bufmgr.*;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.Random;

public class LSHHeaderPage extends HFPage {
   private static final byte HASH_FUNCTION_PAGE = 1;
   private static final byte LAYER_PAGE  = 2;
   private static final byte BASE_PAGE = 3;

   private static final byte pgFld = 1;
   private static final byte bFld = 2 ;
   private static final byte hFld = 1;
   private static final byte LFld = 2;
   private static final byte thFld = 3;
   private static final byte wFld = 4;


   private PageId headerPageId;
   private PageId baseHeaderPageId;

   public LSHHeaderPage(PageId pageId) throws ConstructPageException {
      super();
      this.headerPageId  = pageId;
      super.curPage = pageId;
      try{
         SystemDefs.JavabaseBM.pinPage(pageId, this, false);
      } catch (Exception e) {
         throw new ConstructPageException(e, "pin page failed. failed to construct LSHHeaderPage");
      }

   }

   private LSHHeaderPage()throws ConstructPageException{
      super();
      try{
         Page newPage = new Page();
         PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);
         if (newPageId == null){
            throw new ConstructPageException(null, "new page failed. failed to construct LSHHeaderPage");
         }
         this.init(newPageId, newPage);
         this.headerPageId = newPageId;
      } catch (Exception e){
         throw new ConstructPageException(e, "init failed. failed to construct LSHHeaderPage");
      }
   }


   public LSHHeaderPage(Page page, PageId headerPageId) {
      super(page);
      this.headerPageId = headerPageId;

   }

   public void set_key_type() throws IOException {
//      setSlot(3, (int) AttrType.attrVector100D, 0);
   }

   //   public int getPageType() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException {
//      Tuple t = getRecord(new RID(this.headerPageId, 0));
//
//      return t.getIntFld(pgFld);
//   }
   public int getBaseHeaderPageId() throws FieldNumberOutOfBoundException, IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException {
      Tuple t = getRecord(new RID(this.headerPageId,0));
      short[] shorts = new short[0];
      AttrType[] attrTypes = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
      t.setHdr((short) 2,attrTypes, shorts);
      return t.getIntFld(bFld);
   }

   public int getPageType() throws InvalidSlotNumberException, IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException {
      Tuple t = getRecord(new RID(this.headerPageId,0));
      short[] shorts = new short[0];
      AttrType[] attrTypes = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
      t.setHdr((short) 2,attrTypes, shorts);
      return t.getIntFld(pgFld);
   }

   public int getHashFunctionsPerLayer() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.getPageType() == BASE_PAGE){
         Tuple t = getRecord(new RID(this.headerPageId, 1));
         AttrType[] attrTypes2 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
         t.setHdr((short)4, attrTypes2, null);
         return t.getIntFld(hFld);
      }
      return -1;
   }

   public int getNumLayers() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.getPageType() == BASE_PAGE){
         Tuple t = getRecord(new RID(this.headerPageId, 1));
         AttrType[] attrTypes2 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
         t.setHdr((short)4, attrTypes2, null);
         return t.getIntFld(LFld);
      }
      return -1;
   }
   public int getTotalHashFunctions() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.getPageType() == BASE_PAGE){ // this should be baseHeaderPageId
         Tuple t = getRecord(new RID(this.headerPageId, 1));
         AttrType[] attrTypes2 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
         t.setHdr((short)4, attrTypes2, null);
         return t.getIntFld(thFld);
      }

      return -1;
   }

   public int getBucketsPerHashFunction() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.getPageType() == BASE_PAGE){ // this should be baseHeaderPageId
         Tuple t = getRecord(new RID(this.headerPageId, 1));
         AttrType[] attrTypes2 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
         t.setHdr((short)4, attrTypes2, null);
         return t.getIntFld(wFld);
      }
      return -1;
   }

   private LSHHeaderPage getNextHeaderPage() throws BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, IOException, PageUnpinnedException, ReplacerException {
      PageId nextPageId = this.getNextPage();
      if (nextPageId.pid != INVALID_PAGE) {
         Page nextPage = new Page();
         SystemDefs.JavabaseBM.pinPage(nextPageId, nextPage, false);
         return new LSHHeaderPage(nextPage, nextPageId);
      }
      return null;
   }
   private void unpinPage(PageId pageId, boolean dirty) throws HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
      SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
   }
   private void unpinThisPage(boolean dirty) throws HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
      SystemDefs.JavabaseBM.unpinPage(this.headerPageId,dirty);
   }

   public void getHashFunctions() throws InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HashEntryNotFoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.getPageType() == BASE_PAGE){ // this should be baseHeaderPageId
         LSHHeaderPage nnextHeaderPage = this.getNextHeaderPage();
         if (nnextHeaderPage != null){
            nnextHeaderPage.getHashFunctions();
            return;
         }
         return;
      }
//      Tuple t = getRecord(new RID(this.headerPageId,0));
      if (this.getPageType() == HASH_FUNCTION_PAGE) {
         Tuple bt = getRecord(new RID(this.headerPageId,1));
         bt.setHdr((short)1,new AttrType[]{new AttrType(AttrType.attrInteger)}, null);

         Tuple av = getRecord(new RID(this.headerPageId,2));
         AttrType[] attrTypes = new AttrType[100];
         for (int i = 0;i<100;i++){
            attrTypes[i] = new AttrType(AttrType.attrInteger);
         }
         av.setHdr((short)100,attrTypes,null);

         int[] a = new int[100];
         int b = bt.getIntFld(1);
         for(int i = 1; i<=100;i++){
            a[i-1] = av.getIntFld(i);
         }
         LSHashFunction lsHashFunction = new LSHashFunction(a, b);
         LSHashFunctionsMap.addHashFunction(lsHashFunction);
      }
      LSHHeaderPage nextHeaderPage = this.getNextHeaderPage();
      this.unpinThisPage(false);
      if (nextHeaderPage != null){
         nextHeaderPage.getHashFunctions();
      }
      return;
   }

   public void getLayers(int h) throws BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, IOException, PageUnpinnedException, ReplacerException, FieldNumberOutOfBoundException, InvalidSlotNumberException, HashEntryNotFoundException, InvalidTupleSizeException, InvalidTypeException {
      if (this.headerPageId.pid == this.getBaseHeaderPageId()){// this should be baseHeaderPageId
         LSHHeaderPage nnextHeaderPage = this.getNextHeaderPage();
         if (nnextHeaderPage != null){
            nnextHeaderPage.getLayers(h);
            return;
         }
         return;
      }

//      Tuple t = getRecord(new RID(this.headerPageId,0));
      if (this.getPageType() == LAYER_PAGE) {
         Tuple layerSt = getRecord(new RID(this.headerPageId,1));
         layerSt.setHdr((short)2 , new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)},null);

         Tuple hfList = getRecord(new RID(this.headerPageId,2));
         AttrType[] attrTypes = new AttrType[h];
         for (int i = 0;i<h;i++){
            attrTypes[i] = new AttrType(AttrType.attrInteger);
         }
         hfList.setHdr((short)h,attrTypes,null);
         int[] hashFunctions = new int[h];
         for (int i = 1; i <= h; i++) {
            hashFunctions[i-1] = hfList.getIntFld(i);
         }
         LSHLayer l = new LSHLayer(hashFunctions,layerSt.getIntFld(1),layerSt.getIntFld(2));
         LSHLayerMap.addLayer(l);
      }


      LSHHeaderPage nextHeaderPage = this.getNextHeaderPage();
      this.unpinThisPage(false);
      if (nextHeaderPage != null){
         nextHeaderPage.getLayers(h);
      }
      return;
   }

   private void setBaseHeaderPage(int pid, int h, int L, int th, int w) throws FieldNumberOutOfBoundException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
      Tuple t1= new Tuple();
      Tuple t2 = new Tuple();

      short[] shorts = new short[0];
      AttrType[] attrTypes1 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
      AttrType[] attrTypes2 = {new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)};
      t1.setHdr((short)2,attrTypes1, shorts);
      t2.setHdr((short)4,attrTypes2, shorts);

      t1.setIntFld(pgFld, BASE_PAGE);
      t1.setIntFld(bFld, pid);
      t2.setIntFld(hFld, h);
      t2.setIntFld(LFld, L);
      t2.setIntFld(thFld, th);
      t2.setIntFld(wFld, w);
//      SystemDefs.JavabaseBM.pinPage(pid,this,true);

      RID rid = this.insertRecord(t1.getTupleByteArray());
      RID rid2 = this.insertRecord(t2.getTupleByteArray());
//      SystemDefs.JavabaseBM.unpinPage(this.headerPageId, true);
   }

   private void setHashFunctionHeaderPage(int baseHeaderPid,int b, int[] a) throws FieldNumberOutOfBoundException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
      Tuple t1 = new Tuple();
      t1.setHdr((short) 2, new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},null);
      t1.setIntFld(pgFld, HASH_FUNCTION_PAGE);
      t1.setIntFld(bFld, baseHeaderPid);

      Tuple t2 = new Tuple();
      t2.setHdr((short)1,new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
      t2.setIntFld(1, b);

      Tuple t3 = new Tuple();
      AttrType[] attrTypes = new AttrType[a.length];
      for (int i = 0;i<a.length;i++){
         attrTypes[i] = new AttrType(AttrType.attrInteger);
      }
      t3.setHdr((short)a.length,attrTypes,null);
      for(int i =0;i<a.length;i++){
         t3.setIntFld(i+1, a[i]);
      }

      RID rid1 = this.insertRecord(t1.getTupleByteArray());
      RID rid2 = this.insertRecord(t2.getTupleByteArray());
      RID rid3 = this.insertRecord(t3.getTupleByteArray());
//      SystemDefs.JavabaseBM.unpinPage(this.headerPageId, true);
   }

   private void setLayerHeaderPage(int baseHeaderPid, int layerStartPid,int layerId, int[] hashes) throws FieldNumberOutOfBoundException, IOException, InvalidTupleSizeException, InvalidTypeException {
      Tuple t1 = new Tuple();
      Tuple t2 = new Tuple();
      Tuple t3 = new Tuple();

      t1.setHdr((short)2 , new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)},null);
      t1.setIntFld(pgFld, LAYER_PAGE);
      t1.setIntFld(bFld,  baseHeaderPid);

      t2.setHdr((short)2 , new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger)},null);
      t2.setIntFld(1, layerStartPid);
      t2.setIntFld(2, layerId);

      AttrType[] attrTypes = new AttrType[hashes.length];
      for (int i = 0;i<hashes.length;i++){
         attrTypes[i] = new AttrType(AttrType.attrInteger);
      }
      t3.setHdr((short)hashes.length,attrTypes,null);
      for (int i =0;i<hashes.length;i++){
         t3.setIntFld(i+1, hashes[i]);
      }

      RID rid1 = this.insertRecord(t1.getTupleByteArray());
      RID rid2 = this.insertRecord(t2.getTupleByteArray());
      RID rid3 = this.insertRecord(t3.getTupleByteArray());
//      SystemDefs.JavabaseBM.unpinPage(this.headerPageId, true);
   }



   public static class LSHHeaderPageFactory {

      private static LSHHeaderPage createBaseHeaderPage(int h, int L) throws ConstructPageException, IOException, HashEntryNotFoundException, FieldNumberOutOfBoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
         LSHHeaderPage startHeaderPage = new LSHHeaderPage();
         startHeaderPage.setBaseHeaderPage(startHeaderPage.getCurPage().pid,h, L, 2*h, 100); // th and w can be configurable?
         return startHeaderPage;
      }
      private static LSHHeaderPage createHashFunctionHeaderPage(int startHeaderPageId) throws ConstructPageException, HashEntryNotFoundException, FieldNumberOutOfBoundException, InvalidFrameNumberException, IOException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
         LSHHeaderPage hashfunctionPage = new LSHHeaderPage();
         Random random = new Random();
         int[] a = new int[100];
         int b = random.nextInt(100);
         for(int i = 0; i<100;i++){
            a[i] = random.nextInt(1000); // what should the range of the vector a values and b be?
         }
         hashfunctionPage.setHashFunctionHeaderPage(startHeaderPageId,b,a);
         LSHashFunction hashFunction = new LSHashFunction(a, b);
         LSHashFunctionsMap.addHashFunction(hashFunction);
         return hashfunctionPage;
      }

      private static LSHHeaderPage createLayerHeaderPage(int h, int th, int startHeaderPageId, int layerId) throws ConstructPageException, FieldNumberOutOfBoundException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, InvalidTupleSizeException, InvalidTypeException {
         LSHHeaderPage layerPage = new LSHHeaderPage();
         Random random = new Random();
         int[] hashes = new int[h];
         for (int i =0;i < h; i++){
            int hi  = random.nextInt(th);
            hashes[i] = hi;
         }
         LSHFInnerPage newInnerPage  = new LSHFInnerPage(layerId,0);
         layerPage.setLayerHeaderPage(startHeaderPageId, newInnerPage.getCurPage().pid, layerId,hashes);
         LSHLayer layer = new LSHLayer(hashes, newInnerPage.getCurPage().pid,layerId);
         LSHLayerMap.addLayer(layer);

         SystemDefs.JavabaseBM.unpinPage(newInnerPage.getCurPage(),true); // should it unpin here?
         return layerPage;

      }
      public static LSHHeaderPage createHeaderPages(int h, int L) throws HashEntryNotFoundException, FieldNumberOutOfBoundException, ConstructPageException, InvalidFrameNumberException, IOException, PageUnpinnedException, ReplacerException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, PagePinnedException, InvalidTupleSizeException, InvalidTypeException {
         int th = h*2;
         int w = 100;
         LSHHeaderPage baseHeaderPage = createBaseHeaderPage(h,L);
         int baseHeaderPageId = baseHeaderPage.getCurPage().pid;
         LSHHeaderPage prevHeaderPage = baseHeaderPage;
         for (int i =0;i<th;i++){
            LSHHeaderPage currentHeader =  createHashFunctionHeaderPage(baseHeaderPageId);
            prevHeaderPage.setNextPage(currentHeader.getCurPage());
            SystemDefs.JavabaseBM.unpinPage(prevHeaderPage.getCurPage(),true);
            prevHeaderPage = currentHeader;
         }
         for (int i=0;i<L;i++){
            LSHHeaderPage currentHeader = createLayerHeaderPage(h,th,baseHeaderPageId, i);
            prevHeaderPage.setNextPage(currentHeader.getCurPage());
            SystemDefs.JavabaseBM.unpinPage(prevHeaderPage.getCurPage(),true);
            prevHeaderPage = currentHeader;
         }
         SystemDefs.JavabaseBM.unpinPage(prevHeaderPage.getCurPage(),true);
         SystemDefs.JavabaseBM.pinPage(baseHeaderPage.getCurPage(),baseHeaderPage,false);
         LSHashFunctionsMap.getInstance();
         LSHLayerMap.getInstance();
         return baseHeaderPage;
      }
   }

   //setters for the creating new header pages.

}


/*
   base header page -> hash function page 1 -> hash function page n = th-> layer page 1 -> layer page m
   (a.x + b)%w =>
   L -> (h1, h2, h3, h4)
   L2 -> (h3, h7, h9, h8)

   base header page -> hashfunction pages -> hash page -.... -> layer page -> layer page 2..-> ....
 */

/*
   baseheader page tuple
   (pg page type,baseheaderpage id(b))(h, L, th, w)
 */

/*
   HashFunction Page tuple
   (pg page type, baseHeaderPage Id b)) ( b value of hash function) (100 int values tuple)
 */

/*
   Layer Page tuple
   (pg page type,base Header page id b) (layer start page id, layer ID) (list of hash functions tuple)
 */

/*
   metadata that should be present in the LSHHeaderPage
   pg -> page type
   b -> base header page id
   h -> number of hash functions per layer
   L -> number of layers
   th -> total number of hash functions in the set
   w -> number of buckets per hash function
   [hash function values
   b value of the hash function] * th times
   [layer layout -> list of hash funciton ids that would be used for the layer, start page of each layer] * L times

   th = h * 2 ->
 */


// slot numbers start from 0
// fld numbers start from 1