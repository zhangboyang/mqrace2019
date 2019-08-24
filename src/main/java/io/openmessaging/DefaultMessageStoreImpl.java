package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
	
    private static void doSortMessage(ArrayList<Message> a)
    {
    	Collections.sort(a, tComparator);
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
    
    private static final String storagePath = "./";
//    private static final String storagePath = "/alidata1/race2019/data/";
    
    private static final RandomAccessFile tAxisData;
    private static final RandomAccessFile tAxisBodyData;
    private static final RandomAccessFile aAxisData;
    static {
    	RandomAccessFile t, tb, a;
    	try {
			t = new RandomAccessFile(storagePath + "tAxis.point.data", "rw");
			t.setLength(0);
			tb = new RandomAccessFile(storagePath + "tAxis.body.data", "rw");
			tb.setLength(0);
			a = new RandomAccessFile(storagePath + "aAxis.point.data", "rw");
			a.setLength(0);
			
		} catch (IOException e) {
			t = null;
			tb = null;
			a = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisData = t;
    	tAxisBodyData = tb;
    	aAxisData = a;
    }
    

    private static final int MAXMSG = 2100000000;
    private static final int N_TSLICE = 5000;
    private static final int N_ASLICE = 1000;
    
    private static final int TSLICE_INTERVAL = MAXMSG / N_TSLICE;
    
    private static int tSliceCount = 0;
    private static long tSlicePivot[] = new long[N_TSLICE + 1];
    private static int tSliceRecordCount[] = new int[N_TSLICE + 1];
    private static int tSliceRecordOffset[] = new int[N_TSLICE + 1];
    
    
    private static long aSlicePivot[] = new long[N_ASLICE + 1];

    
    private static final int blockCountTable[][] = new int[N_TSLICE][N_ASLICE];
    private static final int blockOffsetTableAxisT[][] = new int[N_TSLICE][N_ASLICE];
    private static final int blockOffsetTableAxisA[][] = new int[N_TSLICE][N_ASLICE];
    
    private static int insCount = 0;
    private static int tAxisWriteCount = 0;
    private static int aAxisWriteCount = 0;

    
    private static long globalMaxA = Long.MIN_VALUE;
    private static long globalMinA = Long.MAX_VALUE;
    
    
    
    private static ArrayList<Message> writeBuffer = new ArrayList<Message>();
    
    
    private static final int READBUFSZ = 10240;
    
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
    		writeBuffer.clear();

    		long aLimit = aSlicePivot[aSliceId + 1];
    		
    		for (int tSliceId = 0; tSliceId < tSliceCount; tSliceId++) {
//    			System.out.println(String.format("%d %d: %016X", aSliceId, tSliceId, peekFrontA(tSliceId)));
    			assert peekFrontA(tSliceId) >= aSlicePivot[aSliceId];
    			while (peekFrontA(tSliceId) < aLimit) {
    				writeBuffer.add(getFront(tSliceId));
    				blockCountTable[tSliceId][aSliceId]++;
    			}
    		}
    		
    		Collections.sort(writeBuffer, tComparator);
    		
    		System.out.println(String.format("a-slice %d: pivot=%d limit=%d count=%d", aSliceId, aSlicePivot[aSliceId], aLimit, writeBuffer.size()));
    		
    		ByteBuffer pointBuffer = ByteBuffer.allocate(writeBuffer.size() * 16);
    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
    		for (int i = 0; i < writeBuffer.size(); i++) {
    			aAxisWriteCount++;
    			Message m = writeBuffer.get(i);
    			pointBuffer.putLong(m.getT());
    			pointBuffer.putLong(m.getA());
    		}
    		aAxisData.write(pointBuffer.array());
    	}
    	System.out.println("[" + new Date() + "]: build A-axis index finished");
    }
    
    
    
    
    

    private static void flushWriteBuffer(long exclusiveT) throws IOException
    {
		ByteBuffer pointBuffer = ByteBuffer.allocate(writeBuffer.size() * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		ByteBuffer bodyBuffer = ByteBuffer.allocate(writeBuffer.size() * 34);
		bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
		
		
//		System.out.println(String.format("flush=%d size=%d", tSliceCount - 1, writeBuffer.size()));
		int nWrite;
		for (nWrite = 0; nWrite < writeBuffer.size(); nWrite++) {
			Message curMessage = writeBuffer.get(nWrite);
			if (curMessage.getT() == exclusiveT) {
				break;
			}
		}
		
		Collections.sort(writeBuffer.subList(0, nWrite), aComparator);
		
		for (int i = 0; i < nWrite; i++) {
			Message curMessage = writeBuffer.get(i);
			tAxisWriteCount++;
			pointBuffer.putLong(curMessage.getT());
			pointBuffer.putLong(curMessage.getA());
			bodyBuffer.put(curMessage.getBody());
		}
		
		tSliceRecordCount[tSliceCount - 1] = nWrite;
		writeBuffer.subList(0, nWrite).clear();
		
		tAxisData.write(pointBuffer.array(), 0, nWrite * 16);  // 此处可以异步写来改进性能
		tAxisBodyData.write(bodyBuffer.array(), 0, nWrite * 34);
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
		
		writeBuffer.add(message);
		
    	insCount++;
    	if (insCount % 1000000 == 0) {
			System.out.println("[" + new Date() + "]: " + String.format("ins %d: %s", insCount, dumpMessage(message)));
		}
    }
    
    private static void postInsertProcess() throws IOException
    {
    	flushWriteBuffer(Long.MAX_VALUE);
    	tSlicePivot[tSliceCount] = Long.MAX_VALUE;
    	assert writeBuffer.isEmpty();
    	
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
    }

    private static volatile boolean putFinished = false;
    private static void sortThreadProc()
    {
    	System.out.println("[" + new Date() + "]: sort thread started");
    	
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
    	


//    	//为最后的查询平均值预热JVM
//    	getAvgValue(aMin, aMax, tMin, tMax);
//    	if (firstFlag) {
//    		for (int i = 0; i < 30000; i++) {
//    			getAvgValue(aMin, aMax, tMin, tMax);
//    		}
//    	}
    	

    	return result;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    	return 0;
    }
    
    private static void atShutdown()
    {
    	System.out.println("[" + new Date() + "]: shutdown hook");
    }
}
