
package edu.buffalo.cse.sql.index;

import java.awt.List;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;
 
public class ISAMIndex implements IndexFile {
  
	public ManagedFile file;
	public IndexKeySpec keySpec;
	
	
	
  public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
    throws IOException, SqlException
  {
	  this.file = file;
	  this.keySpec = keySpec;
	  
	  
	  // throw new SqlException("Unimplemented");
  }
  
  public static ISAMIndex create(FileManager fm,
                                 File path,
                                 Iterator<Datum[]> dataSource,
                                 IndexKeySpec key)
    //throws SqlException, IOException
  {
	  try{
		  ManagedFile managedFile = fm.open(path);
		  int numberOfDataPages = 0;
		  int numberOfNodePages = 0;
		  Map<Integer, Datum[]> nodePages = new LinkedHashMap<Integer, Datum[]>();
		 
		  //new code
		  Map<Datum, Datum[]> pageRecords = new LinkedHashMap<Datum, Datum[]>();
		  
		  managedFile.ensureSize(numberOfDataPages+1);
		  DatumBuffer datumNodeBuffer = null;
		  DatumBuffer datumDataBuffer = null;
		  Datum[] lastDataWritten = null;
		  Datum[] row = null;
		  int schema_length = 0;
		  Schema.Type[] column_type = null;
		  int leaf_pages;
		  while(dataSource.hasNext()){
			  try{
				  row = dataSource.next();
				  if(schema_length == 0){
					  schema_length = row.length;
					  column_type = new Schema.Type[schema_length];
					  for(int i=0; i<schema_length; i++){
						  column_type[i] = row[i].getType();
					  }
					  datumDataBuffer = new DatumBuffer(managedFile.getBuffer(numberOfDataPages), column_type);
					  datumDataBuffer.initialize(8);
					  managedFile.safePin((numberOfDataPages));
					  DatumSerialization.write(managedFile.getBuffer(numberOfDataPages), 0, new Datum.Int(0));
				  }
				  datumDataBuffer.write(row);
			  }catch(InsufficientSpaceException spaceException1){
				
				  //new code for creating ISAM leaf nodes
				  pageRecords.put(new Datum.Int(numberOfDataPages), key.createKey(lastDataWritten));
				//  managedFile.flush();
				  managedFile.unpin(numberOfDataPages, true); 
				  numberOfDataPages++;
				  managedFile.resize(numberOfDataPages+1);
				  datumDataBuffer = new DatumBuffer(managedFile.getBuffer(numberOfDataPages), column_type);
				  datumDataBuffer.initialize(8);
				  managedFile.safePin(numberOfDataPages);
				  DatumSerialization.write(managedFile.getBuffer(numberOfDataPages), 0, new Datum.Int(0));
				  datumDataBuffer.write(row);
			  }finally{
				  lastDataWritten = row;
			  }
		  }
		  
		  pageRecords.put(new Datum.Int(numberOfDataPages), key.createKey(lastDataWritten));
		  managedFile.unpin(numberOfDataPages, true);
		  
		  leaf_pages = numberOfDataPages;
		  
		  //code for creating the index nodes
		  Map<Datum, Datum[]> tempPageRecords = new LinkedHashMap<Datum, Datum[]>();
		  Schema.Type[] index_type = null;
		  Datum[] data = null;
		  Datum[] tempData = null;
		  int pass=1;
		  
		  do{
			  Iterator mapIterator = pageRecords.entrySet().iterator();
			  int recordFlag = 0;
			  index_type=null;
			  data=null;
			  while(mapIterator.hasNext()){
				  Map.Entry record = (Map.Entry)mapIterator.next();
				  if(recordFlag == 0){
					  index_type = new Schema.Type[((Datum[])record.getValue()).length+1];
					  index_type[0] = Schema.Type.INT;
					  for(int i=0; i<((Datum[])record.getValue()).length; i++){
						  index_type[i+1] = ((Datum[])record.getValue())[i].getType();
					  }
					  recordFlag = 1;
					  numberOfDataPages++;
					  managedFile.resize(numberOfDataPages+1);
					  datumDataBuffer = new DatumBuffer(managedFile.getBuffer(numberOfDataPages), index_type);
					  datumDataBuffer.initialize(8);
					  managedFile.safePin(numberOfDataPages);
				  }
				  data = new Datum[((Datum[])record.getValue()).length+1];
				  data[0] = (Datum)record.getKey();
				  
				  for(int i=0; i<((Datum[])record.getValue()).length; i++){
					  data[i+1] = ((Datum[])record.getValue())[i];
				  }
				  try{
					  datumDataBuffer.write(data);
				  }catch(InsufficientSpaceException spaceException4){
					tempData = new Datum[lastDataWritten.length-1];
					for(int i=1; i<lastDataWritten.length;i++){
						tempData[i-1] = lastDataWritten[i];
					}
					tempPageRecords.put(new Datum.Int(numberOfDataPages), tempData);
					managedFile.unpin(numberOfDataPages, true);
					numberOfDataPages++;
					managedFile.resize(numberOfDataPages+1);
					datumDataBuffer = new DatumBuffer(managedFile.getBuffer(numberOfDataPages), index_type);
					datumDataBuffer.initialize(8);
					managedFile.safePin(numberOfDataPages);
					datumDataBuffer.write(data);
				  }finally{
					  lastDataWritten = data;
				  }
			  }
			  tempData = new Datum[lastDataWritten.length-1];
			  for(int i=1; i<lastDataWritten.length;i++){
			 	 tempData[i-1] = lastDataWritten[i];
		      }
			  tempPageRecords.put(new Datum.Int(numberOfDataPages), tempData);
			  managedFile.unpin(numberOfDataPages, true);
			  pass++;
			  pageRecords = new HashMap<Datum, Datum[]>(tempPageRecords);
			  Iterator tempIterator = tempPageRecords.entrySet().iterator();
			  while(tempIterator.hasNext()){
				  Map.Entry pairs = (Map.Entry)tempIterator.next();
				  tempIterator.remove();
			  }
		  }while(pageRecords.size() > 1);
		  
		  
		  
		  DatumSerialization.write(managedFile.getBuffer(managedFile.size()-1), 0, new Datum.Int(leaf_pages));
		  
		  int rootDataLength = (new DatumBuffer(managedFile.getBuffer(numberOfDataPages), index_type)).length();
		  System.out.println("Flushing data to the file. Please wait..");
		  managedFile.flush();
		  System.out.println("File is ready for use.");
		  return null;
	  }catch(IOException ioexception){
		  ioexception.printStackTrace();
		  return null;
	  }catch(Exception exception){
		  exception.printStackTrace();
		  return null;
	  }
	  
    //throw new SqlException("Unimplemented");
  }
  
  public IndexIterator scan() 
    throws SqlException, IOException
  {
	  
	  int maxLeafPages = DatumSerialization.read(this.file.getBuffer(this.file.size()-1), 0, Schema.Type.INT).toInt();
	  
	  DatumStreamIterator iterator = new DatumStreamIterator(this.file, this.keySpec.rowSchema());
	  iterator.currPage(0);
	  iterator.currRecord(0);
	  int max_record = new DatumBuffer(this.file.getBuffer(maxLeafPages), this.keySpec.rowSchema()).length()-1;
	  iterator.maxPage(maxLeafPages);
	  iterator.maxRecord((new DatumBuffer(this.file.getBuffer(maxLeafPages), this.keySpec.rowSchema())).read(max_record));
	  return iterator.ready();
    //throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException
  {
	  
	  DatumStreamIterator iterator = new DatumStreamIterator(this.file, this.keySpec.rowSchema());
	  iterator.currPage(0);
	  iterator.currRecord(0);
	  
	  int dataPage = getPage(toKey);
	  
	  DatumBuffer currentPageBuffer = null;
	  currentPageBuffer = new DatumBuffer(this.file.getBuffer(dataPage), this.keySpec.rowSchema());
	  int leafPageLength = currentPageBuffer.length();
	  Datum[] indexKeys = null;
	  Datum[] returnData = null;
	  for(int i=0; i<leafPageLength; i++){
		  indexKeys = this.keySpec.createKey(currentPageBuffer.read(i));
		  if(Arrays.equals(indexKeys, toKey)){
			  returnData = currentPageBuffer.read(i);
			  break;
		  }
		  if (!Arrays.equals(indexKeys, toKey) && Datum.compareRows(indexKeys, toKey)< 0){
			  returnData = currentPageBuffer.read(i);
		  }
	  }
	  
	  iterator.maxPage(dataPage);
	  iterator.maxRecord(returnData);
    return iterator.ready();
  }

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException
  {
	  int maxLeafPages = DatumSerialization.read(this.file.getBuffer(this.file.size()-1), 0, Schema.Type.INT).toInt();
	  
	  DatumStreamIterator iterator = new DatumStreamIterator(this.file, this.keySpec.rowSchema());
	  int dataPage = getPage(fromKey);
	  DatumBuffer currentPageBuffer = null;
	  currentPageBuffer = new DatumBuffer(this.file.getBuffer(dataPage), this.keySpec.rowSchema());
	  int leafPageLength = currentPageBuffer.length();
	  Datum[] indexKeys = null;
	  int row = 0,i=0;
	  for(i=0; i<leafPageLength; i++){
		  indexKeys = this.keySpec.createKey(currentPageBuffer.read(i));
		  if(Arrays.equals(indexKeys, fromKey)){
			  row = i;
			  break;
		  }
		  if (!Arrays.equals(indexKeys, fromKey) && Datum.compareRows(indexKeys, fromKey) < 0){
			  row++;
		  }
	  }
	  iterator.currPage(dataPage);
	  iterator.currRecord(row);
	  
	  int max_record = new DatumBuffer(this.file.getBuffer(maxLeafPages), this.keySpec.rowSchema()).length()-1;
	  iterator.maxPage(maxLeafPages);
	  iterator.maxRecord((new DatumBuffer(this.file.getBuffer(maxLeafPages), this.keySpec.rowSchema())).read(max_record));
	  return iterator.ready();
  }

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException
  {
	  DatumStreamIterator iterator = new DatumStreamIterator(this.file, this.keySpec.rowSchema());
	  int startPage = getPage(start);
	  iterator.currPage(startPage);
	  int startRow = 0;
	  
	  DatumBuffer currentPageBuffer = null;
	  currentPageBuffer = new DatumBuffer(this.file.getBuffer(startPage), this.keySpec.rowSchema());
	  int leafPageLength = currentPageBuffer.length();
	  Datum[] indexKeys = null;
	  Datum[] returnData = null;
	  for(int i=0; i<leafPageLength; i++){
		  indexKeys = this.keySpec.createKey(currentPageBuffer.read(i));
		  if(Arrays.equals(indexKeys, start)){
			  startRow = i;
			  break;
		  }
		  if (!Arrays.equals(indexKeys, start) & Datum.compareRows(indexKeys, start) < 0){
			  startRow++;
		  }
	  }
	  
	  iterator.currRecord(startRow);
	  
	  
	  
	  
	  
	  int endPage = getPage(end);
	  
	  currentPageBuffer = new DatumBuffer(this.file.getBuffer(endPage), this.keySpec.rowSchema());
	  leafPageLength = currentPageBuffer.length();
	  for(int i=0; i<leafPageLength; i++){
		  indexKeys = this.keySpec.createKey(currentPageBuffer.read(i));
		  if(Arrays.equals(indexKeys, end))
			  returnData = currentPageBuffer.read(i);
		  if (!Arrays.equals(indexKeys, end) & Datum.compareRows(indexKeys, end)<0)
			  returnData = currentPageBuffer.read(i);
	  }
	  
	  
	  iterator.maxPage(endPage);
	  iterator.maxRecord(returnData);
	  return iterator.ready();
  }

  public Datum[] get(Datum[] key)
    throws SqlException, IOException
  {
	  int rootPageNumber = this.file.size() - 1;
	  Schema.Type[] index_type = new Schema.Type[key.length+1];
	  index_type[0] = Schema.Type.INT;
	  for(int i=0; i<key.length; i++){
		  index_type[i+1] = key[i].getType();
	  }
	  
	  DatumBuffer rootDatumBuffer = new DatumBuffer(this.file.getBuffer(rootPageNumber), index_type);
	  Datum[] currentData = null;
	  DatumBuffer currentPageBuffer = null;
	  int currentPage = rootPageNumber;
	  int pageLength;
	  int nextLookUpPage = 0;
	  do{
		  if((DatumSerialization.read(this.file.getBuffer(currentPage), 0, Schema.Type.INT)).toInt() == 0){
			  break;
		  }
		  currentPageBuffer = new DatumBuffer(this.file.getBuffer(currentPage), index_type);
		  pageLength = currentPageBuffer.length();
		  Datum previousKey = currentPageBuffer.read(0)[1];
		  if(key[0].toInt() < previousKey.toInt()){
			  nextLookUpPage = currentPageBuffer.read(0)[0].toInt();
		  }else{
			  for(int i=1; i<pageLength; i++){
				  currentData = currentPageBuffer.read(i);
				  if(previousKey.toInt() < key[0].toInt() && key[0].toInt() < currentData[1].toInt()){
					  nextLookUpPage = currentData[0].toInt();
					  break;
				  }
				  previousKey = currentData[1];
			  }
		  } 
		  currentPage = nextLookUpPage;
	  }while(true);
	  
	  //retrieve the value from the leaf page now
	  currentPageBuffer = new DatumBuffer(this.file.getBuffer(currentPage), this.keySpec.rowSchema());
	  int leafPageLength = currentPageBuffer.length();
	  Datum[] indexKeys = null;
	  Datum[] returnData = null;
	  for(int i=0; i<leafPageLength; i++){
		  indexKeys = this.keySpec.createKey(currentPageBuffer.read(i));
		  if(Arrays.equals(indexKeys, key))
			  returnData = currentPageBuffer.read(i);
	  }
	  return returnData;
	  // throw new SqlException("Unimplemented");
  }  
  
  public int getPage(Datum[] key) throws SqlException, IOException{
	  int rootPageNumber = this.file.size() - 1;
	  Schema.Type[] index_type = new Schema.Type[key.length+1];
	  index_type[0] = Schema.Type.INT;
	  for(int i=0; i<key.length; i++){
		  index_type[i+1] = key[i].getType();
	  }
	  
	  DatumBuffer rootDatumBuffer = new DatumBuffer(this.file.getBuffer(rootPageNumber), index_type);
	  Datum[] currentData = null;
	  DatumBuffer currentPageBuffer = null;
	  int currentPage = rootPageNumber;
	  int pageLength;
	  int nextLookUpPage = 0;
	  do{
		  if((DatumSerialization.read(this.file.getBuffer(currentPage), 0, Schema.Type.INT)).toInt() == 0){
			  break;
		  }
		  currentPageBuffer = new DatumBuffer(this.file.getBuffer(currentPage), index_type);
		  pageLength = currentPageBuffer.length();
		  Datum previousKey = currentPageBuffer.read(0)[1];
		  if(key[0].toInt() < previousKey.toInt()){
			  nextLookUpPage = currentPageBuffer.read(0)[0].toInt();
		  }else{
			  for(int i=1; i<pageLength; i++){
				  currentData = currentPageBuffer.read(i);
				  if(previousKey.toInt() < key[0].toInt() && key[0].toInt() < currentData[1].toInt()){
					  nextLookUpPage = currentData[0].toInt();
					  break;
				  }
				  previousKey = currentData[1];
			  }
		  } 
		  currentPage = nextLookUpPage;
	  }while(true);
	  return currentPage;
  }
}