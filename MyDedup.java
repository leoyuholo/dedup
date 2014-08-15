import java.io.*;
import java.util.*;
import java.lang.*;
import java.lang.Byte;
import java.util.concurrent.ArrayBlockingQueue;
import java.lang.InterruptedException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.rackspacecloud.client.cloudfiles.FilesAccountInfo;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesConstants;
import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesContainerExistsException;
import com.rackspacecloud.client.cloudfiles.FilesContainerInfo;
import com.rackspacecloud.client.cloudfiles.FilesContainerNotEmptyException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;
/**
 * @author csci4180-10@fall_2012
 *
 */

public class MyDedup{
	
	// constant variables
	static final int READ_QUEUE_CAPACITY = 1048576;
	static final int THREAD_QUEUE_CAPACITY = 1048576;
	static final int READ_BUFFER_SIZE = 1048576;
	static final int REGISTRYHASHMAP_SIZE = 1048576;
	static final int FILEHASHMAP_SIZE = 1048576;
	static final int CHUNKLIST_SIZE = 1048576;
	static final long MODULO = 0x100000000L;
	
	// swift properties
	static final String USERNAME = "csci4180-10:csci4180-10";
	static final String PASSWORD = "20120927";
	static final String AUTHENTICATION_URL = "http://10.10.10.1:5000/v2.0/tokens";
	static final String AUTHENTICATION_VER = "v2.0";
	static final String CONTAINERNAME = "asg3_19";
	
	static FilesClient filesClient = null;
	
	static int chunkIDCounter = 0;
	
	// chunk for upload to container
	private static class Chunk{
		
		ChunkRegistry chunkRegistry = null;
		byte[] payload = null;
		
		public Chunk(ChunkRegistry aChunkRegistry, byte[] content){
			
			chunkRegistry = aChunkRegistry;
			payload = content;
			
		}
		
	}
	
	// chunk registry for existence of a specific chunk
	private static class ChunkRegistry{
		
		String chunkID = null;
		int chunkSize = 0;
		int referenceCount = 0;
		Boolean uploaded = false;
		Object uploadedLock = null;
		Integer downloadingCount = 0;
		Object downloadingCountLock = null;
		
		public ChunkRegistry(String id, int size){
			
			chunkID = id;
			chunkSize = size;
			referenceCount = 1;
			uploaded = false;
			uploadedLock = new Object();
			downloadingCount = 0;
			downloadingCountLock = new Object();
			
		}
		
	}
	
	// information for retrieving a chunk for download and deletion from container
	private static class RetrieveChunkInfo{
		
		ChunkRegistry chunkRegistry = null;
		String checksum = null;
		LinkedList<Long> chunkOffsetList = null;
		
		public RetrieveChunkInfo(ChunkRegistry registry, String md, long offset){
			
			chunkRegistry = registry;
			checksum = md;
			chunkOffsetList = new LinkedList<Long>();
			chunkOffsetList.addFirst(offset);
			
		}
		
	}
	
	// thread registry for thread join
	private static class ThreadRegistry{
		
		Thread thread = null;
		
		public ThreadRegistry(Thread aThread){
			
			thread = aThread;
			
		}
		
	}
	
	private static HashMap<String, ChunkRegistry> registryHashMap = new HashMap<String, ChunkRegistry>(REGISTRYHASHMAP_SIZE);
	private static HashMap<String, HashMap<String, RetrieveChunkInfo> > fileHashMap = new HashMap<String, HashMap<String, RetrieveChunkInfo> >(FILEHASHMAP_SIZE);
	private static ArrayBlockingQueue<ThreadRegistry> threadsArrayBlockingQueue = new ArrayBlockingQueue<ThreadRegistry>(THREAD_QUEUE_CAPACITY);
	
	private static class FileReader implements Runnable{
		
		protected BufferedInputStream inStream = null;
		protected ArrayBlockingQueue<Byte> readQueue = null;
		protected long fileLength = 0;
		protected long byteRead = 0;
		
		public FileReader(File file, ArrayBlockingQueue<Byte> buffer){
			
			try{
				
				inStream = new BufferedInputStream(new FileInputStream(file), 104857600);
				fileLength = file.length();
				byteRead = 0;
				readQueue = buffer;
				
			}catch(FileNotFoundException e){
				
				e.printStackTrace();
				
			}
			
		}
		
		public void run(){
			
			try{
				
				while(byteRead < fileLength){
					
					readQueue.put((byte)inStream.read());
					byteRead++;
					
				}
				
			}catch(Exception e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static class Uploader implements Runnable{
		
		protected Chunk chunk = null;
		
		public Uploader(Chunk aChunk){
			
			chunk = aChunk;
			
		}
		
		public void run(){
				
			// upload to container
			try{
				
				Map<String, String> mp = new HashMap<String, String>();
				filesClient.storeObject(CONTAINERNAME, chunk, "", chunk.chunkRegistry.chunkID, mp);
				
				synchronized(chunk.chunkRegistry.uploadedLock){
				
					// finished upload, wake up waiting threads if any
					chunk.chunkRegistry.uploaded = true;
					chunk.chunkRegistry.uploadedLock.notifyAll();
					
				}
				
			}catch(Exception e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static class Dedup implements Runnable{
		
		protected ArrayBlockingQueue<Byte> readQueue = null;
		
		protected String filename = null;
		protected long fileLength = 0;
		protected long byteRead = 0;
		
		protected int windowSize = 0;
		protected int multiplier = 0;
		protected long modulo = MODULO;
		
		protected long dPowmModq = 0;
		protected long rfp = 0;
		
		protected long anchorMask = 0;
		protected int maxChunkSize = 0;
		
		protected byte[] chunkBuffer = null;
		protected int chunkPos = 0;
		
		MessageDigest md = null;
		byte[] cs = null;
		String checksum = null;
		HashMap<String, RetrieveChunkInfo> chunkList = null;
		
		// statistic
		protected int totalChunks = 0;
		protected int totalUniqueChunks = 0;
		protected int bytesWithDedup = 0;
		
		public Dedup(ArrayBlockingQueue<Byte> readBuffer, 
					 String fname, long fileLen, int m, int d, int b, int x){
			
			readQueue = readBuffer;
			
			filename = fname;
			fileLength = fileLen;
			byteRead = 0;
			
			windowSize = m;
			multiplier = d;
			
			// pre-compute d^m % q
			dPowmModq = 1;
			
			while(windowSize > 0){
				
				if(windowSize % 2 == 1){
					
					dPowmModq = (dPowmModq * multiplier) % modulo;
					
				}
				
				windowSize = (windowSize - (windowSize % 2)) >> 1;
				
				multiplier = (int)(((long)multiplier * (long)multiplier) % modulo);
				
			}
			
			windowSize = m;
			multiplier = d;
			
			rfp = 0;
			
			anchorMask = ~0x00;
			anchorMask <<= b;
			anchorMask = ~anchorMask;
			
			maxChunkSize = x;
			
			chunkBuffer = new byte[maxChunkSize];
			chunkPos = 0;
			
			try{
				
				md = MessageDigest.getInstance("SHA-1");
				
			}catch(NoSuchAlgorithmException e){
				
				e.printStackTrace();
				
			}
			
			cs = null;
			checksum = null;
			
			chunkList = new HashMap<String, RetrieveChunkInfo>(CHUNKLIST_SIZE);
			
			totalChunks = 0;
			totalUniqueChunks = 0;
			bytesWithDedup = 0;
			
		}
		
		public void run(){
			
			try{
				
				ChunkRegistry chunkRegistryReturenValue = null;
				RetrieveChunkInfo retrieveChunkInfoReturnValue = null;
				HashMap<String, RetrieveChunkInfo> chunkListReturnValue = null;
				String chunkID = null;
				Uploader uploader = null;
				Thread uploaderThread = null;
								
				while(byteRead < fileLength){
					
					// insert an item to chunkBuffer
					chunkBuffer[chunkPos] = readQueue.take().byteValue();
					
					chunkPos++;
					byteRead++;
					
					// if enough items in chunkBuffer to compute rfp
					if(chunkPos == windowSize){
						
						// rfp0
						rfp = chunkBuffer[0] % modulo;
						for(int i = 1;i < windowSize;i++){
						
							rfp = (rfp * multiplier) % modulo;
							rfp = (rfp + chunkBuffer[i]) % modulo;
							
						}
						
					}else if(chunkPos > windowSize){
						
						// rfp1
						rfp = (rfp * multiplier) % modulo;
						rfp = (rfp - chunkBuffer[chunkPos - windowSize - 1] * dPowmModq) % modulo;
						rfp = (rfp + chunkBuffer[chunkPos - 1]) % modulo;
						
					}
					
					// if rfp match or reach maximum chunkBuffer size or reach end of file
					if((chunkPos >= windowSize && (rfp & anchorMask) == 0x0L) || chunkPos == maxChunkSize || byteRead == fileLength){
						
						// record total number of chunks
						totalChunks++;
						
						// compute checksum
						md.update(chunkBuffer, 0, chunkPos);
						cs = md.digest();
						checksum = new String(cs);
						
						// test if chunkBuffer exist
						chunkRegistryReturenValue = registryHashMap.get(checksum);
						
						if(chunkRegistryReturenValue == null){
							
							// chunkBuffer not exist, record statistic, put into hashmap and upload
							
							totalUniqueChunks++;
							bytesWithDedup += chunkPos;
							
							chunkID = new String(Integer.toString(chunkIDCounter));
							chunkIDCounter++;
							
							// put checksum into registryHashMap
							chunkRegistryReturenValue = new ChunkRegistry(chunkID, chunkPos);
							registryHashMap.put(checksum, chunkRegistryReturenValue);
							
							// create an Uploader thread to upload
							uploader = new Uploader(new Chunk(chunkRegistryReturenValue, Arrays.copyOf(chunkBuffer, chunkPos));
							uploaderThread = new Thread(uploader);
							
							// add uploaderThread to threadsArrayBlockingQueue
							threadsArrayBlockingQueue.put(new ThreadRegistry(uploaderThread));
							
							// uploaderThread start to work
							uploaderThread.start();
							
						}else{
							
							// chunkBuffer exists, increment reference count and get chunkID
							
							chunkRegistryReturenValue.referenceCount++;
							
						}
						
						// put checksum into chunkList
						retrieveChunkInfoReturnValue = chunkList.get(checksum);
						
						if(retrieveChunkInfoReturnValue == null){
							
							// if not exis in chunkList, add to chunkList
							
							chunkList.put(checksum, new RetrieveChunkInfo(chunkRegistryReturenValue, checksum, byteRead - chunkPos));
							
						}else{
							
							// if exist in chunkList, add offset to chunkOffsetList
							
							retrieveChunkInfoReturnValue.chunkOffsetList.addFirst(byteRead - chunkPos);
							
						}
						
						// reset items in chunkBuffer
						chunkPos = 0;
						
					}
					
				}
				
				chunkListReturnValue = fileHashMap.put(new String(filename), chunkList);
				
				// print out statistic
				System.out.println("\nReport output:");
				System.out.println("Total number of chunks: " + totalChunks);
				System.out.println("Number of unique chunks: " + totalUniqueChunks);
				System.out.println("Number of bytes with deduplication: " + bytesWithDedup);
				System.out.println("Number of bytes without deduplication: " + byteRead);
				System.out.printf("Deduplcation ratio: %.2f%%\n", ((float)(byteRead - bytesWithDedup) / (float)byteRead) * 100.0f);
				
			}catch(InterruptedException e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static boolean upload(int m, int d, int b, int x, String filename){
		
		ArrayBlockingQueue<Byte> readBuffer = new ArrayBlockingQueue<Byte>(READ_QUEUE_CAPACITY);
		
		File file = new File(filename);
		
		if(!file.exists()){
			
			System.out.println("File not find: " + filename);
			
			return false;
			
		}
		
		if(registryHashMap.get(filename) != null){
			
			System.out.println("File exist: " + filename);
			
			return false;
			
		}
		
		FileReader fileReader = new FileReader(file, readBuffer);
		Dedup dedup = new Dedup(readBuffer, filename, file.length(), m, d, b, x);
		
		Thread fileReaderThread = new Thread(fileReader);
		Thread dedupThread = new Thread(dedup);
		
		// fileReaderThread start to work
		fileReaderThread.start();
		
		// dedupThread start to work
		dedupThread.start();
		
		try{
			
			// wait until fileReaderThread finished
			fileReaderThread.join();
			
			// wait until dedupThread finished
			dedupThread.join();
			
		}catch(InterruptedException e){
			
			e.printStackTrace();
			
		}
		
		return true;
		
	}
	
	private static class Downloader implements Runnable{
		
		RetrieveChunkInfo retrieveChunkInfo = null;
		RandomAccessFile writer = null;
		byte[] downloadBuffer = null;
		
		public Downloader(RetrieveChunkInfo chunkInfo, File file){
			
			retrieveChunkInfo = chunkInfo;
			
			try{
			
				writer = new RandomAccessFile(file, "rw");
				
			}catch(IOException e){
			
				e.printStackTrace();
				
			}
			
		}
		
		public void run(){
			
			try{
				
				synchronized(retrieveChunkInfo.chunkRegistry.uploadedLock){
					
					// wait untill chunk uploaded
					if(retrieveChunkInfo.chunkRegistry.uploaded == false){
						
						retrieveChunkInfo.chunkRegistry.uploadedLock.wait();
						
					}
					
				}
				
				// download from container
				downloadBuffer = filesClient.getObject(CONTAINERNAME, retrieveChunkInfo.chunkRegistry.chunkID);
				
				synchronized(retrieveChunkInfo.chunkRegistry.downloadingCountLock){
					
					// decrement downloadingCount when finished download
					retrieveChunkInfo.chunkRegistry.downloadingCount--;
					
					// wake up waiting threads if any
					retrieveChunkInfo.chunkRegistry.downloadingCountLock.notifyAll();
					
				}
				
				// write content to corresponding offset
				long chunkOffset = 0;
				Iterator<Long> iterator = retrieveChunkInfo.chunkOffsetList.iterator();
				
				while(iterator.hasNext()){
					
					chunkOffset = iterator.next();
					
					// write to disk
					writer.seek(chunkOffset);
					writer.write(downloadBuffer);
					
				}
				
			}catch(Exception e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static boolean download(String filename){
		
		File file = new File(filename);
		
		if(file.exists()){
			
			System.out.println("File exists, going to overwrite");
			
		}
		
		HashMap<String, RetrieveChunkInfo> chunkListReturnValue = null;
		
		chunkListReturnValue = fileHashMap.get(filename);
		
		if(chunkListReturnValue == null){
			
			System.out.println("No such file");
			
			return false;
			
		}
		
		// statistic
		int downloadedChunks = 0;
		int downloadedBytes = 0;
		int reconstructedBytes = 0;
		
		Collection<RetrieveChunkInfo> collection = chunkListReturnValue.values();
		Iterator<RetrieveChunkInfo> iterator = collection.iterator();
		RetrieveChunkInfo retrieveChunkInfo = null;
		Downloader downloader = null;
		Thread downloaderThread = null;
		
		try{
			
			while(iterator.hasNext()){
				
				retrieveChunkInfo = iterator.next();
				
				// increment downloadingCount before threadownloaderThread start to work
				retrieveChunkInfo.chunkRegistry.downloadingCount++;
				
				// create a downloaderThread to download file
				downloader = new Downloader(retrieveChunkInfo, file);
				downloaderThread = new Thread(downloader);
				
				// add downloaderThread to threadsArrayBlockingQueue
				threadsArrayBlockingQueue.put(new ThreadRegistry(downloaderThread));
				
				// downloaderThread start to work
				downloaderThread.start();
				
				// record statistic
				downloadedChunks++;
				downloadedBytes += retrieveChunkInfo.chunkRegistry.chunkSize;
				reconstructedBytes += (retrieveChunkInfo.chunkRegistry.chunkSize * retrieveChunkInfo.chunkOffsetList.size());
				
			}
			
		}catch(InterruptedException e){
			
			e.printStackTrace();
			
		}
		
		// print out statistic
		System.out.println("\nReport output:");
		System.out.println("Number of chunks downloaded: " + downloadedChunks);
		System.out.println("Number of bytes downloaded: " + downloadedBytes);
		System.out.println("Number of bytes reconstructed: " + reconstructedBytes);
		
		return true;
		
	}
	
	private static class Deleter implements Runnable{
		
		protected RetrieveChunkInfo retrieveChunkInfo = null;
		
		public Deleter(RetrieveChunkInfo aRetrieveChunkInfo){
			
			retrieveChunkInfo = aRetrieveChunkInfo;
			
		}
		
		public void run(){
			
			// delete from container
			try{
				
				synchronized(retrieveChunkInfo.chunkRegistry.uploadedLock){
					
					// wait untill chunk uploaded
					if(retrieveChunkInfo.chunkRegistry.uploaded == false){
						
						retrieveChunkInfo.chunkRegistry.uploadedLock.wait();
						
					}
					
				}
				
				synchronized(retrieveChunkInfo.chunkRegistry.downloadingCountLock){
				
					// wait untill not threads downloading such chunk
					while(retrieveChunkInfo.chunkRegistry.downloadingCount > 0){
						
						retrieveChunkInfo.chunkRegistry.downloadingCountLock.wait();
						
					}
					
				}
				
				filesClient.deleteObject(CONTAINERNAME, retrieveChunkInfo.chunkRegistry.chunkID);
				
			}catch(Exception e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static boolean delete(String filename){
		
		HashMap<String, RetrieveChunkInfo> chunkListReturnValue = null;
		
		chunkListReturnValue = fileHashMap.get(filename);
		
		if(chunkListReturnValue == null){
			
			System.out.println("No such file");
			
			return false;
			
		}
		
		// statistic
		int deletedChunks = 0;
		int deletedBytes = 0;
		
		// do file deletion
		Collection<RetrieveChunkInfo> collection = chunkListReturnValue.values();
		Iterator<RetrieveChunkInfo> iterator = collection.iterator();
		RetrieveChunkInfo retrieveChunkInfo = null;
		int listSize = 0;
		Deleter deleter = null;
		Thread deleterThread = null;
		
		while(iterator.hasNext()){
			
			retrieveChunkInfo = iterator.next();
			
			listSize = retrieveChunkInfo.chunkOffsetList.size();
			retrieveChunkInfo.chunkRegistry.referenceCount -= listSize;
			
			if(retrieveChunkInfo.chunkRegistry.referenceCount <= 0){
				
				// create a thread to delete file
				deleter = new Deleter(retrieveChunkInfo);
				deleterThread = new Thread(deleter);
				
				// add deleterThread to threadsArrayBlockingQueue
				try{
				
					threadsArrayBlockingQueue.put(new ThreadRegistry(deleterThread));
				
				}catch(InterruptedException e){
					
					e.printStackTrace();
					
				}
				
				// deleterThread start to work
				deleterThread.start();
				
				registryHashMap.remove(retrieveChunkInfo.checksum);
				
				deletedChunks++;
				deletedBytes += retrieveChunkInfo.chunkRegistry.chunkSize;
				
			}
			
		}
		
		fileHashMap.remove(filename);
		
		// print out statistic
		System.out.println("\nReport output:");
		System.out.println("Number of chunks deleted: " + deletedChunks);
		System.out.println("Number of bytes deleted: " + deletedBytes);
		
		return true;
		
	}
	
	private static class ThreadJoiner implements Runnable{
		
		public ThreadJoiner(){
		
		}
		
		public void run(){
			
			ThreadRegistry threadRegistry = null;
					
			try{
				
				// wait until all threads finished
				while(true){
					
					threadRegistry = threadsArrayBlockingQueue.take();
					
					// check if terminate
					if(threadRegistry.thread == null){
						
						break;
						
					}
					
					threadRegistry.thread.join();
					
				}
				
			}catch(InterruptedException e){
				
				e.printStackTrace();
				
			}
			
		}
		
	}
	
	private static void commandLineInterface(){
		
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		
		String line = null;
		String filename = null;
		
		int m = 0;
		int d = 0;
		int b = 0;
		int x = 0;
		
		String[] parameters = null;
		
		while(true){
			
			System.out.print("> ");
			System.out.flush();
			
			try{
				
				line = bufferedReader.readLine();
				
			}catch(IOException e){
				
				e.printStackTrace();
				
			}
			
			if(line.equals("exit")){
				
				break;
				
			}
				
			parameters = line.split(" ");
			
			if(parameters[0].equals("upload")){
				
				if(parameters.length!=6){
					
					System.out.println("upload <m> <d> <b> <x> <path name of the file>");
					System.out.println("download <path name of the file>");
					System.out.println("delete <path name of the file>");
					
				}else{
					
					m = Integer.parseInt(parameters[1]);
					d = Integer.parseInt(parameters[2]);
					b = Integer.parseInt(parameters[3]);
					x = Integer.parseInt(parameters[4]);
					
					filename = parameters[5];
					
					upload(m, d, b, x, filename);
					
				}
				
			}else if(parameters[0].equals("download")){
				
					filename = parameters[1];
					
					download(filename);
				
			}else if (parameters[0].equals("delete")){
						
				filename = parameters[1];
				
				delete(filename);
				
			}else{
				
				System.out.println("upload <m> <d> <b> <x> <path name of the file>");
				System.out.println("download <path name of the file>");
				System.out.println("delete <path name of the file>");
				
			}
			
		}
		
	}
	
	public static void main(String[] args){
		
		// login
		try{
		
			filesClient = new FilesClient(USERNAME, PASSWORD, AUTHENTICATION_URL, AUTHENTICATION_VER);
			// filesClient.setConnectionTimeOut(150);
			filesClient.setConnectionTimeOut(10000);
			filesClient.login();
			
			if(filesClient.containerExists(CONTAINERNAME)){
				
				System.out.println("Container exists");
				
			}else{
			
				filesClient.createContainer(CONTAINERNAME);
				
			}
			
		}catch(Exception e){
		
			e.printStackTrace();
			
		}
		
		ThreadJoiner threadJoiner = new ThreadJoiner();
		Thread threadJoinerThread = new Thread(threadJoiner);
		
		// threadJoinerThread start to work
		threadJoinerThread.start();
		
		// enter command line interface
		commandLineInterface();
		
		try{
			
			// insert terminator to threadsArrayBlockingQueue
			threadsArrayBlockingQueue.put(new ThreadRegistry(null));
			
			// wait until threadJoinerThread finished
			threadJoinerThread.join();
		
		}catch(InterruptedException e){
			
			e.printStackTrace();
			
		}
		
		return;
		
	}
	
}
