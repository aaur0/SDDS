package code.sdds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.ColumnPath;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.KeyspaceServiceImpl;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Hello world!
 *
 */
public class CassandraClient 
{
	private static String MINHASH_KS = "MINHASH_KS";
	
	private static String FULLHASH_CF = "FULLHASH_CF";
	private static String FILERECIPE_CF = "FILERECIPE_CF";
	private static String CHUNKDATA_CF = "CHUNKDATA_CF";
	
	private static String[] MINHASH_CFS = {FULLHASH_CF, FILERECIPE_CF, CHUNKDATA_CF};
	
	private static String FILE_KS = "FILE_KS";
	
	private static String FILEMINHASH_CF = "FILEMINHASH_CF";
	
	private static Cluster cassandraCluster = null;
	//private HConnectionManager connectionManager;
	//private CassandraHostConfigurator cassandraHostConfigurator;
	private String clusterName = "TestCluster";
	
	
	private static Keyspace MINHASHKeySpace;
	private static Keyspace FILEKeySpace;
	
	
	private static Properties props;
	
	/**
	 * 
	 */
	public void initCluster() {
		cassandraCluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");
		//cassandraHostConfigurator = new CassandraHostConfigurator("127.0.0.1:9170");
	    //connectionManager = new HConnectionManager(clusterName,cassandraHostConfigurator);
	}
	
	
	/**
	 * ToDo 1 : add Comparators and Validators to KeySpace and Column Family ! 
	 */
	public void createKeySpaceAndColumnFamilies() {
		List<ColumnFamilyDefinition> CFDefList = null; 
		ColumnFamilyDefinition CFDef = null;
		KeyspaceDefinition KSDef;
		
		if(cassandraCluster.describeKeyspace(MINHASH_KS) == null) { 
		    
			CFDefList = new ArrayList<ColumnFamilyDefinition>();
			
			/* Column Family - LIST OF FULL HASHES for a given MINHASH */
			/* FULL HASHES are dynamically added for every snapshot */
		    CFDef = HFactory.createColumnFamilyDefinition(MINHASH_KS, FULLHASH_CF, ComparatorType.BYTESTYPE);
		    CFDefList.add(CFDef);
		    
		    /* Column Family - FILE NAME and List of CHUNK IDs for given MINHASH */
		    /* FILE NAME and chunk list are added dynamically */
		    CFDef = HFactory.createColumnFamilyDefinition(MINHASH_KS, FILERECIPE_CF, ComparatorType.BYTESTYPE);
		    CFDefList.add(CFDef);
		    
		    /* Column Family - MERGED CHUNK LIST and their Data*/
		    /* added Dynamically */
		    CFDef = HFactory.createColumnFamilyDefinition(MINHASH_KS, CHUNKDATA_CF, ComparatorType.BYTESTYPE);
		    CFDefList.add(CFDef);
		    
		    KSDef = HFactory.createKeyspaceDefinition(MINHASH_KS, "org.apache.cassandra.locator.SimpleStrategy", 1, CFDefList);
		    cassandraCluster.addKeyspace(KSDef);
		    		    
		}
		
		if(cassandraCluster.describeKeyspace(FILE_KS) == null) { 
			
			CFDefList = new ArrayList<ColumnFamilyDefinition>();
			
			CFDef = new BasicColumnFamilyDefinition();
			CFDef.setKeyspaceName(FILE_KS);
		    CFDef.setName(FILEMINHASH_CF);  
		    CFDefList.add(new ThriftCfDef(CFDef));
			
		    KSDef = HFactory.createKeyspaceDefinition(FILE_KS, "org.apache.cassandra.locator.SimpleStrategy", 1, CFDefList);
		    cassandraCluster.addKeyspace(KSDef);
		    
		}
	}
	
	public void initKeySpaceService() { 
		MINHASHKeySpace = HFactory.createKeyspace(MINHASH_KS, cassandraCluster);
		FILEKeySpace = HFactory.createKeyspace(FILE_KS, cassandraCluster);
	}
	
	/**
	 * 
	 */
	public void describeAllKeySpaces() {
		List<KeyspaceDefinition> defs = cassandraCluster.describeKeyspaces();
		Iterator<KeyspaceDefinition> itr = defs.iterator();
		while(itr.hasNext()) {
			KeyspaceDefinition ksp = itr.next();
			System.out.println(ksp.getName() + " " + cassandraCluster.describeKeyspace(ksp.getName())); // + " " + ksp.getReplicationFactor());
		}
	}
	
	/**
	 * 
	 */
	public void cleanUp() {
		if(cassandraCluster.describeKeyspace(MINHASH_KS) != null) {
			cassandraCluster.dropKeyspace(MINHASH_KS);
		}
		if(cassandraCluster.describeKeyspace(FILE_KS) != null) {
			cassandraCluster.dropKeyspace(FILE_KS);
		}
	}
	
	/**
	 * TODO : adding fullHashes as column names in FULLHASH_CF column family !! 
	 */
	public void insertFullHash(byte[] minHash, byte[] fullHash) {
		///
		/* Serializer srz = new Seriali
		ColumnFamilyTemplate<byte[], byte[]> template =
                new ThriftColumnFamilyTemplate<byte[], byte[]>(
                		MINHASHKeySpace, 
                		FULLHASH_CF, 
                		BytesArraySerializer.get(), 
                		BytesArraySerializer.get());
        System.out.println("MINHASHKeySpace " + MINHASHKeySpace);
		ColumnFamilyUpdater<byte[], byte[]> updater = template.createUpdater(minHash);
		updater.setByteArray(fullHash, null);
		System.out.println(template.isColumnsExist(fullHash));
		*/
		/*
		KeyspaceDefinition fromCluster = cassandraCluster.describeKeyspace(MINHASH_KS);
		ColumnFamilyDefinition  cfDef = fromCluster.getCfDefs().
		ColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition(cfDef);
	    BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
	    columnDefinition.setName(StringSerializer.get().toByteBuffer("birthdate"));
	    columnDefinition.setIndexName("birthdate_idx");
	    columnDefinition.setIndexType(ColumnIndexType.KEYS);
	    columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
	    columnFamilyDefinition.addColumnDefinition(columnDefinition);
	    columnDefinition = new BasicColumnDefinition();
	    columnDefinition.setName(StringSerializer.get().toByteBuffer("nonindexed_field"));    
	    columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
	    columnFamilyDefinition.addColumnDefinition(columnDefinition);    
	    cassandraCluster.updateColumnFamily(new ThriftCfDef(columnFamilyDefinition));
	    fromCluster = cassandraCluster.describeKeyspace("DynKeyspace3");
	    */
	}
	
	
	public void inputSnapshot(String fileName) {
		// BinaryFileSplitter binaryFileSplitter = new BinaryFileSplitter();
		// binaryFileSplitter.split(fileName);
	}
		
	/**
	 * 
	 * @param args
	 */
    public static void main( String[] args )
    {
    	CassandraClient caClient = new CassandraClient();
    	caClient.initCluster();
    	// caClient.cleanUp();
    	caClient.createKeySpaceAndColumnFamilies();
    	caClient.describeAllKeySpaces();
    	caClient.initKeySpaceService();
    	System.out.println("------------------");
    	caClient.insertFullHash("cdef".getBytes(), "abcd".getBytes());
    	System.out.println("------------------");

    }
}

