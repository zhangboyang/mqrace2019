package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

	private static boolean pointInRect(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop)
	{
		return rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop;
	}
	
    private static class TComparator implements Comparator<Message> {
        @Override
        public int compare(Message a, Message b) {
            return Long.compare(a.getT(), b.getT());
        }
    }
    private static class AComparator implements Comparator<Message> {
        @Override
        public int compare(Message a, Message b) {
        	return Long.compare(a.getA(), b.getA());
        }
    }
    private static final TComparator tComparator = new TComparator();
    private static final AComparator aComparator = new AComparator();
    
    
    
	private static void printFile(String path)
	{
		System.out.println(String.format("======== %s ========", path)); 
		try {
    		System.out.print(new String(Files.readAllBytes(Paths.get(path))));
    	} catch (Exception e) {
    		System.out.println("READ ERROR!");
    	}
		System.out.println("======== END OF FILE ========");
	}
	
	static {
    	//printFile("/proc/cpuinfo");
    	printFile("/proc/meminfo");
    	System.out.println("Working Directory = " + System.getProperty("user.dir"));
	}
	
	private static String dumpMessage(Message message)
	{
		char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    	StringBuilder s = new StringBuilder();
    	s.append(String.format("%016X,", message.getT()));
    	s.append(String.format("%016X,", message.getA()));
    	byte[] bytes = message.getBody();
    	for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            s.append(HEX_ARRAY[v >>> 4]);
            s.append(HEX_ARRAY[v & 0x0F]);
        }
    	return s.toString();
	}
	
    
    
    private static volatile int state = 0;
    private static final Object stateLock = new Object();
    
    static {
    	Runtime.getRuntime().addShutdownHook(new Thread() {
    		public void run() {
    			atShutdown();
    		}
    	});
    }
    
//    private static final String storagePath = "./";
    private static final String storagePath = "/alidata1/race2019/data/";
    
    private static final String tAxisPointFile = storagePath + "tAxis.point.data";
    private static final String tAxisBodyFile = storagePath + "tAxis.body.data";
    private static final String aAxisPointFile = storagePath + "aAxis.point.data";
    
    private static final RandomAccessFile tAxisData;
    private static final RandomAccessFile tAxisBodyData;
    private static final RandomAccessFile aAxisData;
    
    private static final FileChannel tAxisChannel;
    private static final FileChannel tAxisBodyChannel;
    private static final FileChannel aAxisChannel;
    
    static {
    	RandomAccessFile tFile, tbFile, aFile;
    	FileChannel tChannel, tbChannel, aChannel;
    	try {
			tFile = new RandomAccessFile(tAxisPointFile, "rw");
			tFile.setLength(0);
			tbFile = new RandomAccessFile(tAxisBodyFile, "rw");
			tbFile.setLength(0);
			aFile = new RandomAccessFile(aAxisPointFile, "rw");
			aFile.setLength(0);
			
			tChannel = FileChannel.open(Paths.get(tAxisPointFile));
			tbChannel = FileChannel.open(Paths.get(tAxisBodyFile));
			aChannel = FileChannel.open(Paths.get(aAxisPointFile));
			
		} catch (IOException e) {
			tFile = null;
			tbFile = null;
			aFile = null;
			tChannel = null;
			tbChannel = null;
			aChannel = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisData = tFile;
    	tAxisBodyData = tbFile;
    	aAxisData = aFile;
        tAxisChannel = tChannel;
        tAxisBodyChannel = tbChannel;
        aAxisChannel = aChannel;
    }
    

    private static final int MAXMSG = 2100000000;
    private static final int N_TSLICE = 100000;
    private static final int N_ASLICE = 100;
    
    private static final int TSLICE_INTERVAL = MAXMSG / N_TSLICE;
    
    private static int tSliceCount = 0;
    private static long tSlicePivot[] = new long[N_TSLICE + 1];
    private static int tSliceRecordCount[] = new int[N_TSLICE + 1];
    private static int tSliceRecordOffset[] = new int[N_TSLICE + 1];
    
    
    private static long aSlicePivot[] = new long[N_ASLICE + 1];

    
    private static final int blockCountTable[][] = new int[N_TSLICE][N_ASLICE];
    private static final int blockOffsetTableAxisT[][] = new int[N_TSLICE][N_ASLICE];
    private static final int blockOffsetTableAxisA[][] = new int[N_TSLICE][N_ASLICE];
    private static final long blockPrefixSumBaseTable[][] = new long[N_TSLICE][N_ASLICE];
    
    private static int insCount = 0;
    private static int tAxisWriteCount = 0;
    private static int aAxisWriteCount = 0;

    
    private static long globalMaxA = Long.MIN_VALUE;
    private static long globalMinA = Long.MAX_VALUE;
    
    
    
    
    private static int findSliceT(long tValue)
    {
		int l = 0, r = tSliceCount;
		while (r - l > 1) {
			int m = (l + r) / 2;
			if (tValue >= tSlicePivot[m]) {
				l = m;
			} else {
				r = m;
			}
		}
		return l;
    }
    private static int findSliceA(long aValue)
    {
		int l = 0, r = N_ASLICE;
		while (r - l > 1) {
			int m = (l + r) / 2;
			if (aValue >= aSlicePivot[m]) {
				l = m;
			} else {
				r = m;
			}
		}
		return l;
    }
    
    
    
    
    private static Message writeBuffer[] = new Message[1048576];
    private static int writeBufferPtr = 0;
    private static void putWriteBuffer(Message m)
    {
    	writeBuffer[writeBufferPtr++] = m;
    	if (writeBufferPtr == writeBuffer.length) {
    		Message newBuffer[] = new Message[writeBuffer.length * 2];
    		System.arraycopy(writeBuffer, 0, newBuffer, 0, writeBuffer.length);
    		writeBuffer = newBuffer;
    	}
    }
    private static void clearWriteBuffer()
    {
    	writeBufferPtr = 0;
    }
    
    
    private static final int READBUFSZ = 1280;
    
    private static final int readPtr[] = new int[N_TSLICE];
    
    private static final ByteBuffer readBuffer[] = new ByteBuffer[N_TSLICE];
    private static final int readBufferPtr[] = new int[N_TSLICE];
    private static final int readBufferCap[] = new int[N_TSLICE];
    static {
    	for (int i = 0; i < N_TSLICE; i++) {
    		readBuffer[i] = ByteBuffer.allocate(READBUFSZ * 16);
    		readBuffer[i].order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    
    private static boolean isReadBufferEmpty(int tSliceId)
    {
    	return readBufferPtr[tSliceId] >= readBufferCap[tSliceId];
    }
    private static void refillReadBuffer(int tSliceId) throws IOException
    {
    	if (isReadBufferEmpty(tSliceId)) {
			int remainingCnt = tSliceRecordCount[tSliceId] - readPtr[tSliceId];
			int readCnt = Math.min(remainingCnt, READBUFSZ);
			
			if (readCnt > 0) {
	    		tAxisData.seek((long)(tSliceRecordOffset[tSliceId] + readPtr[tSliceId]) * 16);
	    		tAxisData.read(readBuffer[tSliceId].array(), 0, readCnt * 16);
	    		readPtr[tSliceId] += readCnt;
	    		readBufferPtr[tSliceId] = 0;
	    		readBufferCap[tSliceId] = readCnt;
			}
    	}
    }
    private static long peekFrontA(int tSliceId) throws IOException
    {
    	refillReadBuffer(tSliceId);
    	return isReadBufferEmpty(tSliceId) ? Long.MAX_VALUE : readBuffer[tSliceId].getLong(readBufferPtr[tSliceId] * 16 + 8);
    }
    private static Message getFront(int tSliceId) throws IOException
    {
    	long a = readBuffer[tSliceId].getLong(readBufferPtr[tSliceId] * 16 + 8);
    	long t = readBuffer[tSliceId].getLong(readBufferPtr[tSliceId] * 16);
    	readBufferPtr[tSliceId]++;
    	return new Message(a, t, null);
    }
    
    private static void buildIndexAxisA() throws IOException
    {
    	System.out.println("[" + new Date() + "]: build A-axis index start");
    	for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
    		clearWriteBuffer();
    		

    		long aLimit = aSlicePivot[aSliceId + 1];
    		
    		for (int tSliceId = 0; tSliceId < tSliceCount; tSliceId++) {
//    			System.out.println(String.format("%d %d: %016X", aSliceId, tSliceId, peekFrontA(tSliceId)));
    			assert peekFrontA(tSliceId) >= aSlicePivot[aSliceId];
    			
    			// 取出当前小块的数据
    			int offsetLow = writeBufferPtr;
    			while (peekFrontA(tSliceId) < aLimit) {
    				putWriteBuffer(getFront(tSliceId));
    				blockCountTable[tSliceId][aSliceId]++;
    			}
    			int offsetHigh = writeBufferPtr;
    			
    			// 当前小块已按照a排好序
    			// 对当前小块计算前缀和
    			long prefixSum = blockPrefixSumBaseTable[tSliceId][aSliceId];
    			for (int i = offsetLow; i < offsetHigh; i++) {
    				Message m = writeBuffer[i];
    				long curA = m.getA();
    				prefixSum += curA;
    				m.setA(prefixSum);
    			}
    			if (aSliceId < N_ASLICE - 1) {
    				blockPrefixSumBaseTable[tSliceId][aSliceId + 1] = prefixSum;
    			}
    		}
    		
    		System.out.println(String.format("a-slice %d: pivot=%d limit=%d count=%d", aSliceId, aSlicePivot[aSliceId], aLimit, writeBufferPtr));
    		
    		ByteBuffer pointBuffer = ByteBuffer.allocate(writeBufferPtr * 16);
    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
    		for (int i = 0; i < writeBufferPtr; i++) {
    			aAxisWriteCount++;
    			Message m = writeBuffer[i];
    			pointBuffer.putLong(m.getT());
    			pointBuffer.putLong(m.getA());
    		}
    		aAxisData.write(pointBuffer.array());
    	}
    	System.out.println("[" + new Date() + "]: build A-axis index finished");
    }
    
    private static void buildOffsetTable()
    {
    	int offset;
    	
    	offset = 0;
    	for (int tSliceId = 0; tSliceId < tSliceCount; tSliceId++) {
    		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
    			blockOffsetTableAxisT[tSliceId][aSliceId] = offset;
    			offset += blockCountTable[tSliceId][aSliceId];
    		}
    	}
    	assert offset == insCount;
    	
    	offset = 0;
    	for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
    		for (int tSliceId = 0; tSliceId < tSliceCount; tSliceId++) {
    			blockOffsetTableAxisA[tSliceId][aSliceId] = offset;
    			offset += blockCountTable[tSliceId][aSliceId];
    		}
    	}
    	assert offset == insCount;
    }
    
    
    
    private static class WriteBufferPair {
    	ByteBuffer pointBuffer;
    	ByteBuffer bodyBuffer;
    	int nWrite;
    }
    
    private static final ArrayBlockingQueue<WriteBufferPair> writeQueue = new ArrayBlockingQueue<WriteBufferPair>(10);
    private static volatile boolean writeFinished = false;
    private static void writeThreadProc()
    {
    	System.out.println("[" + new Date() + "]: write thread start");
		try {
	    	while (!writeFinished) {
	    		WriteBufferPair bufferPair = writeQueue.poll(1, TimeUnit.SECONDS);
	    		
	    		if (bufferPair != null) {
	    			tAxisData.write(bufferPair.pointBuffer.array(), 0, bufferPair.nWrite * 16);
	    			tAxisBodyData.write(bufferPair.bodyBuffer.array(), 0, bufferPair.nWrite * 34);
	    		}
	    	}
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}

    	
    	System.out.println("[" + new Date() + "]: write thread finished");
    }
    

    private static void flushWriteBuffer(long exclusiveT) throws IOException
    {
		ByteBuffer pointBuffer = ByteBuffer.allocate(writeBufferPtr * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		ByteBuffer bodyBuffer = ByteBuffer.allocate(writeBufferPtr * 34);
		bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
		
		
//		System.out.println(String.format("flush=%d size=%d", tSliceCount - 1, writeBuffer.size()));
		int nWrite;
		for (nWrite = 0; nWrite < writeBufferPtr; nWrite++) {
			Message curMessage = writeBuffer[nWrite];
			if (curMessage.getT() == exclusiveT) {
				break;
			}
		}
		
		Arrays.sort(writeBuffer, 0, nWrite, aComparator);
		
		for (int i = 0; i < nWrite; i++) {
			Message curMessage = writeBuffer[i];
			tAxisWriteCount++;
			pointBuffer.putLong(curMessage.getT());
			pointBuffer.putLong(curMessage.getA());
			bodyBuffer.put(curMessage.getBody());
		}
		
		tSliceRecordCount[tSliceCount - 1] = nWrite;
		
		System.arraycopy(writeBuffer, nWrite, writeBuffer, 0, writeBufferPtr - nWrite);
		writeBufferPtr -= nWrite;
		
		// 送到写线程去写文件，这样可以排序和写文件同时进行，加快速度
		WriteBufferPair bufferPair = new WriteBufferPair();
		bufferPair.pointBuffer = pointBuffer;
		bufferPair.bodyBuffer = bodyBuffer;
		bufferPair.nWrite = nWrite;
		try {
			writeQueue.put(bufferPair);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
//    private static Message lastMessage = null;
    private static void insertMessage(Message message) throws IOException
    {
//    	if (lastMessage != null) {
//    		assert message.getT() >= lastMessage.getT();
//    	}
//    	lastMessage = message;
    	
    	
    	long curA = message.getA();
    	globalMinA = Math.min(globalMinA, curA);
    	globalMaxA = Math.max(globalMaxA, curA);
    	
		long curT = message.getT();
		
		if (insCount % TSLICE_INTERVAL == 0) {
			if (insCount > 0) {
				flushWriteBuffer(curT);
			}
			tSlicePivot[tSliceCount++] = curT;
			//System.out.println(String.format("t-slice %d: pivot=%d", tSliceCount - 1, tSlicePivot[tSliceCount - 1]));
		}
		
		putWriteBuffer(message);
		
    	insCount++;
    	if (insCount % 1000000 == 0) {
			System.out.println("[" + new Date() + "]: " + String.format("ins %d: %s", insCount, dumpMessage(message)));
		}
    }
    
    private static void postInsertProcess() throws IOException
    {
    	flushWriteBuffer(Long.MAX_VALUE);
    	tSlicePivot[tSliceCount] = Long.MAX_VALUE;
    	assert writeBufferPtr == 0;
    	
    	try {
    		writeFinished = true;
			writeThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
    	// 计算各个块在文件中的偏移
    	for (int i = 0; i < tSliceCount; i++) {
    		if (i > 0) {
    			tSliceRecordOffset[i] = tSliceRecordOffset[i - 1] + tSliceRecordCount[i - 1];
    		}
    		System.out.println(String.format("t-slice %d: pivot=%d count=%d offset=%d", i, tSlicePivot[i], tSliceRecordCount[i], tSliceRecordOffset[i]));
    	}
    	
    	// 计算a轴上的分割点
    	for (int i = 0; i < N_ASLICE; i++) {
    		aSlicePivot[i] = globalMinA + (globalMaxA - globalMinA) / N_ASLICE * i;
    	}
    	aSlicePivot[N_ASLICE] = Long.MAX_VALUE;
    	
    	// 建立a轴上的索引
    	buildIndexAxisA();
    	
    	// 建立内存内索引表
    	buildOffsetTable();
    	
    	// 关闭用于写入的文件
    	tAxisData.close();
    	tAxisBodyData.close();
    	aAxisData.close();
    }

    private static volatile boolean putFinished = false;
    private static void sortThreadProc()
    {
    	System.out.println("[" + new Date() + "]: sort thread started");
    	
		writeThread = new Thread() {
		    public void run() {
		    	writeThreadProc();
		    }
		};
		writeThread.start();
		
    	int nThread = putThreadCount.get();
    	
    	Message buffer[][] = new Message[nThread][];
    	int bufptr[] = new int[nThread];
    	
    	boolean threadExited[] = new boolean[nThread];

    	try {
        	for (int i = 0; i < nThread; i++) {
        		buffer[i] = insertQueue[i].take();
        		bufptr[i] = 0;
        	}
        	
	    	while (true) {
	    		
	    		long minT = Long.MAX_VALUE;
	    		int qid = -1;
	    		
	    		for (int i = 0; i < nThread; i++) {
	    			Message m = buffer[i][bufptr[i]];
	    			if (m != null) {
		    			long curT = m.getT();
		    			if (curT <= minT) {
		    				minT = curT;
		    				qid = i;
		    			}
	    			}
	    		}
	    		
	    		if (qid == -1) break;
	    		
	    		insertMessage(buffer[qid][bufptr[qid]]);
	    		
	    		if (++bufptr[qid] >= buffer[qid].length) {
	    			if (threadExited[qid]) {
	    				buffer[qid] = new Message[1];
	    			} else {
	    				if (putFinished && insertQueue[qid].isEmpty()) {
	    					buffer[qid] = null;
	    				} else {
	    					buffer[qid] = insertQueue[qid].poll(3, TimeUnit.SECONDS);
	    				}
		    			if (buffer[qid] == null) {
		    				System.out.println(String.format("put thread %d timeout, assume exited", qid));
		    				buffer[qid] = putTLD[qid].buffer;
		    				threadExited[qid] = true;
		    			}
	    			}
	    			bufptr[qid] = 0;
	    		}
	    	}
	    	
	    	postInsertProcess();
	    	
	    } catch (InterruptedException | IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
    	System.out.println("[" + new Date() + "]: sort thread finished");
    }

    
    private static final int MAXTHREAD = 100;
    private static final int MAXQUEUE = 20;
    
    private static final ArrayBlockingQueue<Message[]> insertQueue[] = new ArrayBlockingQueue[MAXTHREAD];
    

    
    
    private static final int MAXBUFFER = 1000;
    private static class PutThreadLocalData {
    	Message[] buffer;
    	int bufptr;
    	int threadid;
    	
    	void clear() {
    		buffer = new Message[MAXBUFFER];
    		bufptr = 0;
    	}
    }
    private static final PutThreadLocalData putTLD[] = new PutThreadLocalData[MAXTHREAD];
    private static final AtomicInteger putThreadCount = new AtomicInteger();
    private static final ThreadLocal<PutThreadLocalData> putBuffer = new ThreadLocal<PutThreadLocalData>() {
        @Override protected PutThreadLocalData initialValue() {
        	PutThreadLocalData pd = new PutThreadLocalData();
        	pd.threadid = putThreadCount.getAndIncrement();
        	putTLD[pd.threadid] = pd;
        	insertQueue[pd.threadid] = new ArrayBlockingQueue<Message[]>(MAXQUEUE); 
        	pd.clear();
        	return pd;
        }
    };

    private static Thread sortThread;
    private static Thread writeThread;
    
    @Override
    public void put(Message message) {

    	if (state == 0) {
    		synchronized (stateLock) {
    			if (state == 0) {
					System.out.println("[" + new Date() + "]: put() started");
					
					boolean assertsEnabled = false;
					assert assertsEnabled = true;
					
					System.out.println("assertEnabled=" + assertsEnabled);
					
					
					sortThread = new Thread() {
					    public void run() {
					    	try {
								Thread.sleep(3000);
							} catch (InterruptedException e) {
								e.printStackTrace();
								System.exit(-1);
							}
					    	sortThreadProc();
					    }
					};
					sortThread.start();
					
					state = 1;
    			}
    		}
    	}
    	
    	try {
    		PutThreadLocalData pd = putBuffer.get();
			pd.buffer[pd.bufptr++] = message;
			if (pd.bufptr == pd.buffer.length) {
				insertQueue[pd.threadid].put(pd.buffer);
				pd.clear();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

//    	boolean firstFlag = false;
    	
    	if (state == 1) {
    		synchronized (stateLock) {
    			if (state == 1) {
    				System.out.println("[" + new Date() + "]: getMessage() started");
    				
    				try {
    					putFinished = true;
						sortThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(-1);
					}
    				
    				System.out.println(String.format("insCount=%d", insCount));
    				System.out.println(String.format("tAxisWriteCount=%d", tAxisWriteCount));
    				System.out.println(String.format("aAxisWriteCount=%d", aAxisWriteCount));
    				System.out.println(String.format("globalMinA=%d", globalMinA));
    				System.out.println(String.format("globalMaxA=%d", globalMaxA));
    				
    				
    				System.gc();

    				
//    				firstFlag = true;
    				state = 2;
    			}
    		}
    	}
    	

    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	int aSliceLow = findSliceA(aMin);
    	int aSliceHigh = findSliceA(aMax);
    	
    	
		try {
			
	    	for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
	    		int baseOffset = blockOffsetTableAxisT[tSliceId][aSliceLow];
	    		int nRecord = blockOffsetTableAxisT[tSliceId][aSliceHigh] + blockCountTable[tSliceId][aSliceHigh] - baseOffset;
	    		
	    		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
	    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
	    		ByteBuffer bodyBuffer = ByteBuffer.allocate(nRecord * 34);
	    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
	
				int nPointRead = tAxisChannel.read(pointBuffer, (long)baseOffset * 16);
				int nBodyRead = tAxisBodyChannel.read(bodyBuffer, (long)baseOffset * 34);
				assert nPointRead == nRecord * 16;
				assert nBodyRead == nRecord * 34;
				
				pointBuffer.position(0);
				LongBuffer pointBufferL = pointBuffer.asLongBuffer();
				
				for (int i = 0; i < nRecord; i++) {
					long t = pointBufferL.get();
					long a = pointBufferL.get();
					
					if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
						byte body[] = new byte[34];
						bodyBuffer.position(i * 34);
						bodyBuffer.get(body);
						result.add(new Message(a, t, body));
					}
				}
	    	}
    		
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		Collections.sort(result, tComparator);

		System.out.println("[" + new Date() + "]: " + String.format("queryData: [%d %d] (%d %d %d %d) => %d", tMax-tMin, aMax-aMin, tMin, tMax, aMin, aMax, result.size()));
    	
		
		
//    	//为最后的查询平均值预热JVM
//    	getAvgValue(aMin, aMax, tMin, tMax);
//    	if (firstFlag) {
//    		for (int i = 0; i < 30000; i++) {
//    			getAvgValue(aMin, aMax, tMin, tMax);
//    		}
//    	}
		
		

    	return result;
    }

    
    
    private static class AverageResult {
    	long sum = 0;
    	int cnt = 0;
    }
    
    private static void queryAverageAxisT(AverageResult result, int tSliceId, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = blockOffsetTableAxisT[tSliceId][aSliceLow];
		int nRecord = blockOffsetTableAxisT[tSliceId][aSliceHigh] + blockCountTable[tSliceId][aSliceHigh] - baseOffset;
		
		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisChannel.read(pointBuffer, (long)baseOffset * 16);
		pointBuffer.position(0);
		LongBuffer pointBufferL = pointBuffer.asLongBuffer();
		
		for (int i = 0; i < nRecord; i++) {
			long t = pointBufferL.get();
			long a = pointBufferL.get();
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    
    
    private static void queryAverageAxisA(AverageResult result, int aSliceLow, int aSliceHigh, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
//    	for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
//    		queryAverageAxisT(result, tSliceId, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
//    	}
//    	if (true) return;
//    	System.out.println(String.format("(%d %d %d %d)", tMin, tMax, aMin, aMax));
    			
    	int baseOffsetLow = blockOffsetTableAxisA[tSliceLow][aSliceLow];
		int nRecordLow = blockOffsetTableAxisA[tSliceHigh][aSliceLow] + blockCountTable[tSliceHigh][aSliceLow] - baseOffsetLow;
		
		int baseOffsetHigh = blockOffsetTableAxisA[tSliceLow][aSliceHigh];
		int nRecordHigh = blockOffsetTableAxisA[tSliceHigh][aSliceHigh] + blockCountTable[tSliceHigh][aSliceHigh] - baseOffsetHigh;
		
		ByteBuffer lowBuffer = ByteBuffer.allocate(nRecordLow * 16);
		lowBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisChannel.read(lowBuffer, (long)baseOffsetLow * 16);
		lowBuffer.position(0);
		LongBuffer lowBufferL = lowBuffer.asLongBuffer();
		
		
		ByteBuffer highBuffer = ByteBuffer.allocate(nRecordHigh * 16);
		highBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisChannel.read(highBuffer, (long)baseOffsetHigh * 16);
		highBuffer.position(0);
		LongBuffer highBufferL = highBuffer.asLongBuffer();
		
		int lowOffset = 0;
		int highOffset = 0;
		for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
			
			int lowCount = blockCountTable[tSliceId][aSliceLow];
			int highCount = blockCountTable[tSliceId][aSliceHigh];
			
			long lastPrefixSum = blockPrefixSumBaseTable[tSliceId][aSliceLow];
			long lowSum = lastPrefixSum;
			int lowPtr = -1;
			for (int i = lowOffset; i < lowOffset + lowCount; i++) {
				long prefixSum = lowBufferL.get(i * 2 + 1);
				long a = prefixSum - lastPrefixSum;
				lastPrefixSum = prefixSum;
//				System.out.println(String.format("low t=%d a=%d", lowBufferL.get(i * 2), a));
				if (a < aMin) {
					lowSum = prefixSum;
					lowPtr = i - lowOffset;
				} else {
					break;
				}
			}
			lowPtr++;
			
			lastPrefixSum = blockPrefixSumBaseTable[tSliceId][aSliceHigh];
			long highSum = lastPrefixSum;
			int highPtr = -1;
			for (int i = highOffset; i < highOffset + highCount; i++) {
				long prefixSum = highBufferL.get(i * 2 + 1);
				long a = prefixSum - lastPrefixSum;
//				System.out.println(String.format("high t=%d a=%d", highBufferL.get(i * 2), a));
				lastPrefixSum = prefixSum;
				if (a <= aMax) {
					highSum = highBufferL.get(i * 2 + 1);
					highPtr = i - highOffset;
				}
			}
			
			
			int globalLowPtr = blockOffsetTableAxisT[tSliceId][aSliceLow] + lowPtr;
			int globalHighPtr = blockOffsetTableAxisT[tSliceId][aSliceHigh] + highPtr;
			
			long sum = 0;
			int cnt = 0;
			if (globalHighPtr >= globalLowPtr) {
				sum = highSum - lowSum;
				cnt = globalHighPtr - globalLowPtr + 1;
			}
			
			result.sum += sum;
			result.cnt += cnt;
		
//			AverageResult referenceResult = new AverageResult();
//			queryAverageAxisT(referenceResult, tSliceId, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
////			System.out.println(String.format("tSliceId=%d ; aSliceLow=%d aSliceHigh=%d ; lowPtr=%d  highPtr=%d", tSliceId, aSliceLow, aSliceHigh, lowPtr, highPtr));
////			System.out.println(String.format("cnt=%d sum=%d", cnt, sum));
////			System.out.println(String.format("ref: cnt=%d sum=%d", referenceResult.cnt, referenceResult.sum));
//			assert cnt == referenceResult.cnt;
//			assert sum == referenceResult.sum;

			
			lowOffset += lowCount;
			highOffset += highCount;
		}
		
		assert lowOffset == nRecordLow;
		assert highOffset == nRecordHigh;
    }
    

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	int aSliceLow = findSliceA(aMin);
    	int aSliceHigh = findSliceA(aMax);

    	AverageResult result = new AverageResult();
    	
    	try {
	    	if (tSliceLow == tSliceHigh) {
	    		// 在同一个t块内，只能暴力
	    		queryAverageAxisT(result, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
	    		
	    	} else {
	    		queryAverageAxisT(result, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
	    		queryAverageAxisT(result, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
	    		tSliceLow++;
	    		tSliceHigh--;
	    		if (tSliceLow <= tSliceHigh) {
	    			queryAverageAxisA(result, aSliceLow, aSliceHigh, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
	    		}
	    	}
	    	
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
//    	System.out.println("[" + new Date() + "]: " + String.format("queryAverage: [%d %d] (%d %d %d %d) => %d", tMax-tMin, aMax-aMin, tMin, tMax, aMin, aMax, result.cnt));
    	
    	
    	return result.cnt == 0 ? 0 : result.sum / result.cnt;
    }
    
    private static void atShutdown()
    {
    	System.out.println("[" + new Date() + "]: shutdown hook");
    }
}
