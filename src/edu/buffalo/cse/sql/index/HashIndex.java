package edu.buffalo.cse.sql.index;

/**
 * import statements
 * */
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
 
/**
 * @authtor : Ankur Upadhyay, Computer Science Department, University at Buffalo.
 * public class implementation for creating and readign from the Hash Index on the file
 * containing database tuples.
 */
 
public class HashIndex implements IndexFile {

	/* Managed File object corresponds to the single file on the disk, and gives us 
	*  per page access to the contents of the file.
	*/
	public static ManagedFile file;
	/**
	 * IndexKeySpec object is used to give the relationship between the data tuple and the 
	 * index keys. Like, the createKey() in IndexKeySpec, extracts the key columns of the given
	 * data tuple. For example, of the key were the first column of the relation, IndexKeySpec.createKey()
	 * would return a new Datum[] containing a single element: the contents of the first column.
	 * */
	public static IndexKeySpec keySpec;
  
  	/**
  	 * Constructor defintions.
  	 * */
  	public HashIndex(){} 
  	public HashIndex(ManagedFile file, IndexKeySpec keySpec)
    			throws IOException, SqlException
  	{
	  	HashIndex.file = file;
	  	HashIndex.keySpec = keySpec;
    		//throw new SqlException("Unimplemented");
  	}
  
  /**
   * public method for creating the Hash Index from the file containing the 
   * the data tuples
   * */
  public static HashIndex create(FileManager fm,
                                 File path,
                                 Iterator<Datum[]> dataSource,
                                 IndexKeySpec key,
                                 int directorySize)
    throws SqlException, IOException
  {
	  int numberOfBuckets = directorySize;
	  ManagedFile mf = fm.open(path); //opens the file and return the pointer to the opened file
	  mf.ensureSize(directorySize); //resize the file to the number of pages passed as the argument
	  DatumBuffer datumBuffer;
	  int numberOfPages = numberOfBuckets;
	  /**
	   *  bufferMap is a HashMap which is used to keeps the mapping between the page Number
	   * and the DatumBuffer assosiated with it. Datum Buffer provides the utility methods
	   * for reading and writing to the ByteBuffer.
	   * */
	  Map<Integer, DatumBuffer> bufferMap = new HashMap<Integer, DatumBuffer>();
	  int counter = 0;
	  Schema.Type[] column_type = null; //array for storing the column types
	  while (dataSource.hasNext()){
		  Datum[] data = dataSource.next();
		  if(counter == 0){
			  column_type = new Schema.Type[data.length];
			  /**
			   * fetch the type of the data in the source.
			   * */
			  for(int i=0; i<data.length; i++)
				  column_type[i] = data[i].getType();
			  for(int i= 0; i < mf.size(); i++){
			  	/**
			  	 *  create the instance of the DatumBuffer associated with the page returned by
			  	 *  getBuffer() method in ManagedFile class.
			  	 * */
				  datumBuffer = new DatumBuffer(mf.getBuffer(i), key.rowSchema());
				  /**
				   * initialize the datumBuffer before using. This will delete any row in the
				   * buffer if present initially. We can also reverse the first N bytes of the space
				   * in the buffer for storing metadata, where the N is the parameter passed to the
				   * initialize() method.
				   * */
				  datumBuffer.initialize(8);
				  /**
				   * write the number of pages in the file to the first 4 bytes of the 
				   * byte buffer associated with the first page
				   * */
				  DatumSerialization.write(mf.getBuffer(i), 4, new Datum.Int(directorySize));
				  bufferMap.put(i, datumBuffer); //populate the bufferMap with the mapping of first page and byte buffer
			  }
			  counter = 1;
		  }
		  
		  Datum[] indexKey = key.createKey(data); //fetch the index keys
		  int bucketNumber = (key.hashKey(indexKey))%numberOfBuckets; //fecth the bucket number (page number)
		  int currentPage = bucketNumber;
		  try{
		  	  /**
		  	   * Fetch the page on which the data has to be writtnen.
		  	   * */
			  Datum overflowPage = null;
			  do{
				  overflowPage = DatumSerialization.read(mf.getBuffer(currentPage), 0, Schema.Type.INT);
				  if(overflowPage.toInt() != currentPage+1){
					  currentPage = overflowPage.toInt();
				  }else{
					  break;
				  }
			  }while(true);
			  mf.pin(currentPage); //bring the page to the memory
			  bufferMap.get(currentPage).write(data); // write the data to the buffer map.
			  mf.unpin(currentPage, true); //unpin the page from the memory
		  }catch(InsufficientSpaceException s){
		  	  /**
		  	   * Page is full and is unable to store new records. Hence the 
		  	   * file has to be resized and the new page and its associated Datum 
		  	   * buffer has to be created for writing the data. After writing data to the 
		  	   * new page, the bufferMap has to be updated with the mapping.
		  	   * */
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
  
  /**
   * public method to read the given key from the database using Hash Index
   * */
  public Datum[] get(Datum[] key)
    throws SqlException, IOException
  {
  	  //find the total number of buckets by reading the first 4 bytes of the first page of the file.
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
