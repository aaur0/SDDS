package code.sdds;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Anand Gupta
 * @author Prakash Chandrasekaran
 * @author Gautham Narayanasamy
 * @author VijayaRaghavan Subbaiah
 */
public class BinaryFileSplitter {

	/**
	 * @param args
	 */
	private final int CHUNK_SIZE = 4 * 1024;
	private final String OUTFILE_PREFIX = "binary";
	private String fileName;
	private byte[] fullHash;
	private byte[] minHash;
	private List<byte[]> chunkHash; // also called as FileRecipe
	private Map<byte[], byte[]> chunkHashChunkDataMap;
	
	public BinaryFileSplitter(String fileName) {
		this.fileName = fileName;
		chunkHash = new LinkedList<byte[]>();
		chunkHashChunkDataMap = new HashMap<byte[], byte[]>();  
	}
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		String src = new File(".").getCanonicalPath() + File.separator + "image-src" + File.separator + "source.image";
		BinaryFileSplitter binaryFileSplitter = new BinaryFileSplitter(src);
		src = "D:\\Ubuntu_VirtualBox\\ubuntu11.10.vdi.vdi.vdi" ;
		String dst = new File(".").getCanonicalPath() + File.separator + "image-dest";
		binaryFileSplitter.split(src, dst);

	}
	
	public void split(String sourceBinaryFile, String destFolderName) throws IOException, NoSuchAlgorithmException {
		/**
		 * Checking for destination Folder, else new folder is created in given location 
		 */
		long startTime = System.currentTimeMillis();
		File destDirectory = new File(destFolderName);
		if(! destDirectory.exists()) {
			new File(destFolderName).mkdir();
		}
		
		FileInputStream fin = new FileInputStream(sourceBinaryFile);
		BufferedInputStream bis = new BufferedInputStream(fin);
		long fileNumber = 0;
		FileOutputStream fos; 
		BufferedOutputStream bos;
		byte[] binaryData = new byte[CHUNK_SIZE];
		MessageDigest md = MessageDigest.getInstance("MD5");
		Map<byte[], byte[]> hashDataChunk = new HashMap<byte[], byte[]>();
		Set<byte[]> chunkHashes = new HashSet<byte[]>();
		while(bis.read(binaryData) != -1) {
			//fos = new FileOutputStream(destFolderName + File.separator + OUTFILE_PREFIX + "_" + fileNumber++);
			//bos = new BufferedOutputStream(fos);
			//bos.write(binaryData);
			//bos.close();
			//fos.close();
	         md.update(binaryData); 
	      	 byte[] output = md.digest();
	      	 fileNumber++;
	         // System.out.println(output.length);
	         hashDataChunk.put(output, binaryData);
		}
		bis.close();
		fin.close();
		System.out.println("Total time taken " + (System.currentTimeMillis() - startTime) + " ms");
		System.out.println("Number of Chunks " + fileNumber);
		System.out.println("Number of items in Map " + hashDataChunk.size());
	}
}
