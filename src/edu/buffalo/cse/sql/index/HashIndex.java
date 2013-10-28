
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.Int;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;
 
public class HashIndex implements IndexFile {
	
	public static ManagedFile file;
	public static IndexKeySpec keySpec;
  
  public HashIndex(ManagedFile file, IndexKeySpec keySpec)
    throws IOException, SqlException
  {
	  HashIndex.file = file;
	  HashIndex.keySpec = keySpec;
    //throw new SqlException("Unimplemented");
  }
  
  public static HashIndex create(FileManager fm,
                                 File path,
                                 Iterator<Datum[]> dataSource,
                                 IndexKeySpec key,
                                 int directorySize)
    throws SqlException, IOException
  {
	  int numberOfBuckets = directorySize;
	  ManagedFile mf = fm.open(path);
	  mf.ensureSize(directorySize);
	  DatumBuffer datumBuffer;
	  int numberOfPages = numberOfBuckets;
	  Map<Integer, DatumBuffer> bufferMap = new HashMap<Integer, DatumBuffer>();
	  int counter = 0;
	  Schema.Type[] column_type = null;
	  while (dataSource.hasNext()){
		  Datum[] data = dataSource.next();
		  if(counter == 0){
			  column_type = new Schema.Type[data.length];
			  for(int i=0; i<data.length; i++){
				  column_type[i] = data[i].getType();
			  }
			  for(int i= 0; i < mf.size(); i++){
				  datumBuffer = new DatumBuffer(mf.getBuffer(i), key.rowSchema());
				  datumBuffer.initialize(8);
				  DatumSerialization.write(mf.getBuffer(i), 4, new Datum.Int(directorySize));
				  bufferMap.put(i, datumBuffer);
			  }
			  counter = 1;
		  }
		  
		  
		  Datum[] indexKey = key.createKey(data);
		  int bucketNumber = (key.hashKey(indexKey))%numberOfBuckets;
		  int currentPage = bucketNumber;
		  try{
			  Datum overflowPage = null;
			  do{
				  overflowPage = DatumSerialization.read(mf.getBuffer(currentPage), 0, Schema.Type.INT);
				  if(overflowPage.toInt() != currentPage+1){
					  currentPage = overflowPage.toInt();
				  }else{
					  break;
				  }
			  }while(true);
			  mf.pin(currentPage);
			  bufferMap.get(currentPage).write(data);
			  mf.unpin(currentPage, true);
		  }catch(InsufficientSpaceException s){
			  numberOfPages++;
			  mf.resize(numberOfPages);
			  DatumBuffer datumBufferNew = new DatumBuffer(mf.getBuffer(numberOfPages-1), key.rowSchema());
			  datumBufferNew.initialize(8);
			  DatumSerialization.write(mf.getBuffer(numberOfPages-1), 4, new Datum.Int(directorySize));
			  bufferMap.put(numberOfPages-1, datumBufferNew);
			  Datum newInt = new Datum.Int(numberOfPages-1);
			  DatumSerialization.write(mf.getBuffer(currentPage), 0, newInt);
			  mf.unpin(currentPage, true);
			  mf.pin(numberOfPages-1);
			  bufferMap.get(numberOfPages-1).write(data);
			  mf.unpin(numberOfPages-1, true);
		  }
	  }
	  System.out.println("Flushing data to the file. Please wait..");
	  mf.flush();
	  System.out.println("File is ready for use.");
	  return null;
  }
  
  public IndexIterator scan() 
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public Datum[] get(Datum[] key)
    throws SqlException, IOException
  {
	  Datum numberOfBuckets = DatumSerialization.read(HashIndex.file.getBuffer(0), 4, Schema.Type.INT);
	  int bucketNumber = (HashIndex.keySpec.hashKey(key))%(numberOfBuckets.toInt());
	  DatumBuffer datumBuffer = null;
	  int rowPosition = 0;
	  Datum data[] = null;
	  Datum[] indexKey = null;
	  do{
		  datumBuffer = new DatumBuffer(HashIndex.file.getBuffer(bucketNumber), HashIndex.keySpec.rowSchema());
		  rowPosition = datumBuffer.find(key);
		  data = datumBuffer.read(rowPosition);
		  indexKey = HashIndex.keySpec.createKey(data);
		  if(Arrays.equals(key, indexKey)){
			  break;
		  }else{
			  int tempBucketNumber = bucketNumber;
			  bucketNumber = DatumSerialization.read(HashIndex.file.getBuffer(tempBucketNumber), 0, Schema.Type.INT).toInt();
			  if((bucketNumber < 0) || (bucketNumber == (tempBucketNumber + 1))){
				  return null;
			  }
		  }
	  }while(true);
	  return data;
  }
    
}