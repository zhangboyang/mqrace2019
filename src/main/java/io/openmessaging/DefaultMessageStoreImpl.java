package io.openmessaging;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private static int nextSize(int n)
    {
    	int r = 1;
    	while (r < n) r <<= 1;
    	return r;
    }
    
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
	
	
	// 为给定文件保留*连续*磁盘空间
	private static void reserveDiskSpace(String fileName, long nBytes) throws IOException
	{
		System.out.println("[" + new Date() + "]: " + String.format("reserveDiskSpace: file=%s size=%d", fileName, nBytes));
		// 理论上，用fallocate()系统调用，可以不用写数据而达到预留磁盘空间的目的，但Java8不支持
		// 所以这里使用向文件填0的方法
		byte zeros[] = new byte[4096];
		RandomAccessFile fp = new RandomAccessFile(fileName, "rw");
		fp.setLength(0);
		while (nBytes > 0) {
			int nWrite = (int) Math.min(nBytes, zeros.length);
			fp.write(zeros, 0, nWrite);
			nBytes -= nWrite;
		}
		fp.close();
		System.out.println("[" + new Date() + "]: reserveDiskSpace: done");
	}
	
	
	
//	static {
//    	printFile("/proc/cpuinfo");
//    	printFile("/proc/meminfo");
//    	printFile("/proc/mounts");
//    	System.out.println("Working Directory = " + System.getProperty("user.dir"));
//	}
	
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
    
    private static final String storagePath = "./";
//    private static final String storagePath = "/alidata1/race2019/data/";
    
    private static final String tAxisPointFile = storagePath + "tAxis.point.data";
    private static final String tAxisBodyFile = storagePath + "tAxis.body.data";
    private static final String aAxisIndexFile = storagePath + "aAxis.index.data";
    
    private static final RandomAccessFile tAxisPointData;
    private static final RandomAccessFile tAxisBodyData;
    private static final RandomAccessFile aAxisIndexData;
    
    private static final FileChannel tAxisPointChannel;
    private static final FileChannel tAxisBodyChannel;
    private static final FileChannel aAxisIndexChannel;
    
    static {
    	RandomAccessFile tpFile, tbFile, aIndexFile;
    	FileChannel tpChannel, tbChannel, aIndexChannel;
    	try {
			tpFile = new RandomAccessFile(tAxisPointFile, "rw");
			tpFile.setLength(0);
			tbFile = new RandomAccessFile(tAxisBodyFile, "rw");
			tbFile.setLength(0);
			aIndexFile = new RandomAccessFile(aAxisIndexFile, "rw");
			aIndexFile.setLength(0);
			
			tpChannel = FileChannel.open(Paths.get(tAxisPointFile));
			tbChannel = FileChannel.open(Paths.get(tAxisBodyFile));
			aIndexChannel = FileChannel.open(Paths.get(aAxisIndexFile));
			
		} catch (IOException e) {
			tpFile = null;
			tbFile = null;
			aIndexFile = null;
			tpChannel = null;
			tbChannel = null;
			aIndexChannel = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisPointData = tpFile;
    	tAxisBodyData = tbFile;
    	aAxisIndexData = aIndexFile;
        tAxisPointChannel = tpChannel;
        tAxisBodyChannel = tbChannel;
        aAxisIndexChannel = aIndexChannel;
    }
    

    private static final int MAXMSG = 2100000000;
    private static final int N_TSLICE = 3000000;
    private static final int N_ASLICE = 40;
    
    private static final int TSLICE_INTERVAL = MAXMSG / N_TSLICE;
    
    private static int tSliceCount = 0;
    private static final long tSlicePivot[] = new long[N_TSLICE + 1];
    private static final int tSliceRecordCount[] = new int[N_TSLICE + 1];
    private static final int tSliceRecordOffset[] = new int[N_TSLICE + 1];
    
    
    private static final long aSlicePivot[] = new long[N_ASLICE + 1];
    
    private static final int blockCountTable[][] = new int[N_TSLICE + 1][N_ASLICE + 1];
    private static final int blockOffsetTableAxisT[][] = new int[N_TSLICE + 1][N_ASLICE + 1];
    private static final int blockOffsetTableAxisA[][] = new int[N_TSLICE + 1][N_ASLICE + 1];
    private static final long blockPrefixSumBaseTable[][] = new long[N_TSLICE][N_ASLICE];
    
    private static int insCount = 0;
    
    private static int globalTotalRecords = 0;
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
		assert tSlicePivot[l] <= tValue && tValue < tSlicePivot[l + 1];
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
		assert aSlicePivot[l] <= aValue && aValue < aSlicePivot[l + 1];
		return l;
    }
    
    
    
    
    private static ByteBuffer indexReadBuffer = null;
    private static void reserveIndexReadBuffer(int nBytes)
    {
    	if (indexReadBuffer == null || indexReadBuffer.capacity() < nBytes) {
    		indexReadBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		indexReadBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    private static ByteBuffer indexWriteBuffer = null;
    private static void reserveIndexWriteBuffer(int nBytes)
    {
    	if (indexWriteBuffer == null || indexWriteBuffer.capacity() < nBytes) {
    		indexWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		indexWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    
    private static final int BATCHSIZE = 5000;

    private static void buildIndexForRangeAxisA(int tSliceFrom, int tSliceTo) throws IOException
    {
//    	System.out.println("[" + new Date() + "]: " + String.format("from=%d to=%d", tSliceFrom, tSliceTo));
    	
    	int nRecord = tSliceRecordOffset[tSliceTo + 1] - tSliceRecordOffset[tSliceFrom];
    	reserveIndexReadBuffer(nRecord * 16);
    	reserveIndexWriteBuffer(nRecord * 8);
    	
    	assert tAxisPointData.getFilePointer() == (long)tSliceRecordOffset[tSliceFrom] * 16;
    	tAxisPointData.readFully(indexReadBuffer.array(), 0, nRecord * 16);
    	indexReadBuffer.position(0);
		LongBuffer indexReadBufferL = indexReadBuffer.asLongBuffer();
		
		indexWriteBuffer.position(0);
		LongBuffer indexWriteBufferL = indexWriteBuffer.asLongBuffer();

		
		int sliceRecordCount[] = new int[N_ASLICE];
		int bufferBase[] = new int[N_ASLICE];
		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
			sliceRecordCount[aSliceId] = blockOffsetTableAxisA[tSliceTo + 1][aSliceId] - blockOffsetTableAxisA[tSliceFrom][aSliceId];
			if (aSliceId > 0) {
				bufferBase[aSliceId] = bufferBase[aSliceId - 1] + sliceRecordCount[aSliceId - 1];
			}
		}
		
		int msgPtr = 0;
		for (int tSliceId = tSliceFrom; tSliceId <= tSliceTo; tSliceId++) {
			
			long prefixSum = 0;
			
			for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
				int msgCnt = blockCountTable[tSliceId][aSliceId];
				
				blockPrefixSumBaseTable[tSliceId][aSliceId] = prefixSum;
				
				int putBase = bufferBase[aSliceId] + blockOffsetTableAxisA[tSliceId][aSliceId] - blockOffsetTableAxisA[tSliceFrom][aSliceId];
				for (int i = putBase; i < putBase + msgCnt; i++) {
					
					
					long curA = indexReadBufferL.get((msgPtr++ * 2) + 1);
					prefixSum += curA;
					
					
					indexWriteBufferL.put(i, prefixSum);
				}
			}
		}
		assert msgPtr == nRecord;
		
		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
			aAxisIndexData.seek((long)blockOffsetTableAxisA[tSliceFrom][aSliceId] * 8);
			aAxisIndexData.write(indexWriteBuffer.array(), bufferBase[aSliceId] * 8, sliceRecordCount[aSliceId] * 8);
		}
    }
    
    private static void buildIndexAxisA() throws IOException
    {
    	System.out.println("[" + new Date() + "]: build index for a-axis");
    	
    	tAxisPointData.seek(0);
    	reserveDiskSpace(aAxisIndexFile, (long)insCount * 8); // FIXME: 是否会造成文件在磁盘上存储不连续？
    	
    	for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId += BATCHSIZE) {
    		buildIndexForRangeAxisA(tSliceId, Math.min(tSliceId + BATCHSIZE, tSliceCount) - 1);
    	}
    	
    	System.out.println("[" + new Date() + "]: a-axis index finished");
    	
    	indexReadBuffer = null;
    	indexWriteBuffer = null;
    }
    
    private static void buildOffsetTable()
    {
    	int offset;
    	
    	offset = 0;
    	for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
    		assert offset == tSliceRecordOffset[tSliceId];
    		
    		for (int aSliceId = 0; aSliceId <= N_ASLICE; aSliceId++) {
    			blockOffsetTableAxisT[tSliceId][aSliceId] = offset;
    			offset += blockCountTable[tSliceId][aSliceId];
    		}
    	}
    	assert offset == insCount;
    	
    	offset = 0;
    	for (int aSliceId = 0; aSliceId <= N_ASLICE; aSliceId++) {
    		for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
    			blockOffsetTableAxisA[tSliceId][aSliceId] = offset;
    			offset += blockCountTable[tSliceId][aSliceId];
    		}
    	}
    	assert offset == insCount;
    }
    
    
    
    private static void buildIndex() throws IOException
    {
    	// 计算各个块在文件中的偏移
    	for (int i = 0; i <= tSliceCount; i++) {
    		if (i > 0) {
    			tSliceRecordOffset[i] = tSliceRecordOffset[i - 1] + tSliceRecordCount[i - 1];
    		}
//    		System.out.println(String.format("t-slice %d: pivot=%d count=%d offset=%d", i, tSlicePivot[i], tSliceRecordCount[i], tSliceRecordOffset[i]));
    	}
    	
    	// 建立内存内偏移表
    	buildOffsetTable();
    	
    	// 建立A轴上的索引
    	buildIndexAxisA();
    	
    	// 关闭用于写入的文件
    	tAxisPointData.close();
    	tAxisBodyData.close();
    	aAxisIndexData.close();
    }
    
    
    
    
    
    
    
    
    
    
    private static Message writeBuffer[] = null;
    private static Message writeBuffer2[] = null;
    private static int writeBufferPtr = 0;
    
    private static void reserveWriteBuffer(int n)
    {
    	if (writeBuffer == null || writeBuffer.length < n) {
    		int oldSize;
    		Message newBuffer[] = new Message[nextSize(n)];
    		if (writeBuffer == null) {
    			oldSize = 0;
    		} else {
    			oldSize = writeBuffer.length;
    			System.arraycopy(writeBuffer, 0, newBuffer, 0, oldSize);
    		}
    		for (int i = oldSize; i < newBuffer.length; i++) {
    			newBuffer[i] = new Message(0, 0, new byte[34]);
    		}
    		writeBuffer = newBuffer;
    		writeBuffer2 = new Message[newBuffer.length];
    	}
    }
    private static void shiftWriteBuffer(int n)
    {
    	if (writeBufferPtr != n) {
    		int m = writeBufferPtr - n;
    		
    		System.arraycopy(writeBuffer, n, writeBuffer2, 0, m);
    		System.arraycopy(writeBuffer, 0, writeBuffer2, m, n);
    		System.arraycopy(writeBuffer2, 0, writeBuffer, 0, writeBufferPtr);
    	}
    	writeBufferPtr -= n;
    }
    
    
    private static ByteBuffer pointBuffer = null;
    private static void reservePointBuffer(int nBytes)
    {
    	if (pointBuffer == null || pointBuffer.capacity() < nBytes) {
    		pointBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    
    private static ByteBuffer bodyBuffer = null;
    private static void reserveBodyBuffer(int nBytes)
    {
    	if (bodyBuffer == null || bodyBuffer.capacity() < nBytes) {
    		bodyBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }

    private static void flushWriteBuffer(long exclusiveT) throws IOException
    {
		reservePointBuffer(writeBufferPtr * 16);
		reserveBodyBuffer(writeBufferPtr * 34);
		pointBuffer.position(0);
		bodyBuffer.position(0);
		
		
//		System.out.println(String.format("flush=%d size=%d", tSliceCount - 1, writeBuffer.size()));
		int nWrite;
		for (nWrite = 0; nWrite < writeBufferPtr; nWrite++) {
			Message curMessage = writeBuffer[nWrite];
			if (curMessage.getT() == exclusiveT) {
				break;
			}
		}

		int tSliceId = tSliceCount - 1;
		
		tSliceRecordCount[tSliceId] = nWrite;
		
		// t块内部按a排序
		Arrays.sort(writeBuffer, 0, nWrite, aComparator);
		
		// 计算每小块内记录数量
		int aSliceId = 0;
		for (int i = 0; i < nWrite; i++) {
			Message msg = writeBuffer[i];
			long a = msg.getA();
			
			while (aSliceId < N_ASLICE && a >= aSlicePivot[aSliceId + 1]) aSliceId++;
//			assert aSliceId == findSliceA(a);
			
			blockCountTable[tSliceId][aSliceId]++;
			
			pointBuffer.putLong(msg.getT());
			pointBuffer.putLong(msg.getA());
			bodyBuffer.put(msg.getBody());
		}
		
		shiftWriteBuffer(nWrite);
		
		tAxisPointStream.write(pointBuffer.array(), 0, nWrite * 16);
		tAxisBodyStream.write(bodyBuffer.array(), 0, nWrite * 34);
    }

    private static BufferedOutputStream tAxisPointStream;
    private static BufferedOutputStream tAxisBodyStream;
    private static void beginInsertMessage() throws IOException
    {
    	reserveDiskSpace(tAxisPointFile, (long)globalTotalRecords * 16);
    	tAxisPointStream = new BufferedOutputStream(new FileOutputStream(tAxisPointFile));
    	tAxisBodyStream = new BufferedOutputStream(new FileOutputStream(tAxisBodyFile));
    }
    private static void insertMessage(ByteBuffer buffer, int offset) throws IOException
    {
    	long curT = buffer.getLong(offset);
    	long curA = buffer.getLong(offset + 8);
    	
		if (insCount % TSLICE_INTERVAL == 0) {
			if (insCount > 0) {
				flushWriteBuffer(curT);
			}
			tSlicePivot[tSliceCount++] = curT;
		}
		
		reserveWriteBuffer(writeBufferPtr + 1);
		Message message = writeBuffer[writeBufferPtr++];
		message.setT(curT);
		message.setA(curA);
		System.arraycopy(buffer.array(), offset + 16, message.getBody(), 0, 34);
		
    	insCount++;
    	if (insCount % 1000000 == 0) {
			System.out.println("[" + new Date() + "]: " + String.format("ins %d: %s", insCount, dumpMessage(message)));
		}
    }
    private static void finishInsertMessage() throws IOException
    {
    	flushWriteBuffer(Long.MAX_VALUE);
    	tSlicePivot[tSliceCount] = Long.MAX_VALUE;
    	assert writeBufferPtr == 0;
    	writeBuffer = null;
    	writeBuffer2 = null;
    	
    	tAxisPointStream.close();
    	tAxisPointStream = null;
    	tAxisBodyStream.close();
    	tAxisBodyStream = null;
    }
    



    private static void externalMergeSort() throws IOException
    {
    	System.out.println("[" + new Date().toString() + "]: merge-sort begin!");

		int nThread = putThreadCount.get();
		
		ByteBuffer queueHeadData = ByteBuffer.allocate(64 * nThread); // 还是按64字节对齐一下吧
		queueHeadData.order(ByteOrder.LITTLE_ENDIAN);
		int readCount[] = new int[nThread]; 
		int recordCount[] = new int[nThread];
		long queueHead[] = new long[nThread];
		
		
		for (int i = 0; i < nThread; i++) {
			recordCount[i] = putTLD[i].outputCount;
			readCount[i] = 0;
			putTLD[i].bufferedInputStream = new BufferedInputStream(new FileInputStream(putTLD[i].dataFileName));
			putTLD[i].bufferedInputStream.read(queueHeadData.array(), i * 64, 50);
			queueHead[i] = queueHeadData.getLong(i * 64);
		}
		
		beginInsertMessage();
		
		while (true) {
			
			long minValue = queueHead[0];
			int minPos = 0;
			for (int i = 1; i < nThread; i++) {
				long curValue = queueHead[i];
				if (curValue < minValue) {
					minValue = curValue;
					minPos = i;
				}
			}
			
			if (minValue == Long.MAX_VALUE) {
				break;
			}
			
			insertMessage(queueHeadData, minPos * 64);
			if (++readCount[minPos] >= recordCount[minPos]) {
				queueHead[minPos] = Long.MAX_VALUE;
			} else {
				putTLD[minPos].bufferedInputStream.read(queueHeadData.array(), minPos * 64, 50);
				queueHead[minPos] = queueHeadData.getLong(minPos * 64);
			}
		}
		
		finishInsertMessage();
		
		for (int i = 0; i < nThread; i++) {
			putTLD[i].bufferedInputStream.close();
			putTLD[i].bufferedInputStream = null;
		}
		System.out.println("[" + new Date().toString() + "]: merge-sort completed!");
    }
    
    
    
    
    
    
    
    
    
    private static void calcPivotAxisA()
    {
    	// 计算A的范围
		int nThread = putThreadCount.get();
    	for (int i = 0; i < nThread; i++) {
    		globalMaxA = Math.max(globalMaxA, putTLD[i].maxA);
    		globalMinA = Math.min(globalMinA, putTLD[i].minA);
    	}
		System.out.println(String.format("globalMinA=%d", globalMinA));
		System.out.println(String.format("globalMaxA=%d", globalMaxA));
		
    	// 计算a轴上的分割点
    	for (int i = 0; i < N_ASLICE; i++) {
    		aSlicePivot[i] = globalMinA + (globalMaxA - globalMinA) / N_ASLICE * i;
    	}
    	aSlicePivot[N_ASLICE] = Long.MAX_VALUE;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private static final int MAXTHREAD = 100;

    private static class PutThreadLocalData {
    	BufferedOutputStream bufferedOutputStream;
    	BufferedInputStream bufferedInputStream;
    	
    	ByteBuffer msgData;
    	int outputCount = 0;
    	long maxA = Long.MIN_VALUE;
    	long minA = Long.MAX_VALUE;
    	
    	int threadId;
    	String dataFileName;
    	
    }
    private static final PutThreadLocalData putTLD[] = new PutThreadLocalData[MAXTHREAD];
    private static final AtomicInteger putThreadCount = new AtomicInteger();
    private static final ThreadLocal<PutThreadLocalData> putBuffer = new ThreadLocal<PutThreadLocalData>() {
        @Override protected PutThreadLocalData initialValue() {
        	
        	PutThreadLocalData pd = new PutThreadLocalData();
        	pd.threadId = putThreadCount.getAndIncrement();
        	putTLD[pd.threadId] = pd;
        	pd.dataFileName = String.format("thread%04d.data", pd.threadId);
        	
        	try {
        		pd.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(pd.dataFileName));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
        	
        	pd.msgData = ByteBuffer.allocate(50);
        	pd.msgData.order(ByteOrder.LITTLE_ENDIAN);
        	return pd;
        }
    };
    
    
    private static void flushPutBuffer() throws IOException
    {
    	System.out.println("[" + new Date() + "]: flushing remaining buffers ...");
    	globalTotalRecords = 0;
		int nThread = putThreadCount.get();
		for (int i = 0; i < nThread; i++) {
			PutThreadLocalData pd = putTLD[i];
			
			pd.bufferedOutputStream.close();
			pd.bufferedOutputStream = null;
			
			System.out.println(String.format("thread %d: %d", i, pd.outputCount));
			globalTotalRecords += pd.outputCount;
		}
		System.out.println(String.format("total: %d", globalTotalRecords));
    }

    @Override
    public void put(Message message) {

    	if (state == 0) {
    		synchronized (stateLock) {
    			if (state == 0) {
					System.out.println("[" + new Date() + "]: put() started");
					
					boolean assertsEnabled = false;
					assert assertsEnabled = true;
					
					System.out.println("assertEnabled=" + assertsEnabled);

					state = 1;
    			}
    		}
    	}
    	
    	try {
    		PutThreadLocalData pd = putBuffer.get();
    		long curT = message.getT();
    		long curA = message.getA();
    		pd.msgData.position(0);
    		pd.msgData.putLong(curT);
    		pd.msgData.putLong(curA);
    		pd.msgData.put(message.getBody());
    		
    		pd.outputCount++;
    		pd.maxA = Math.max(pd.maxA, curA);
    		pd.minA = Math.min(pd.minA, curA);
    		pd.bufferedOutputStream.write(pd.msgData.array());
    		
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {

    	if (state == 1) {
    		synchronized (stateLock) {
    			if (state == 1) {
    				System.out.println("[" + new Date() + "]: getMessage() started");

    				
    				try {
    					
    					flushPutBuffer();
						calcPivotAxisA();
						
						externalMergeSort();
						buildIndex();
						
					} catch (Exception e) {
						e.printStackTrace();
						state = -1;
						System.exit(-1);
					}
    				
    				
    				System.out.println(String.format("insCount=%d", insCount));
    				
    				System.gc();

    				state = 2;
    			}
    		}
    	}
    	
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	
    	
		try {
    		// t轴块内没按照a排序，因此要把对应t轴上的块全部读进来
    		// 就算是按a排序了并只读取需要的a轴上的块，因为t轴分块分得很细，也不会有性能提升
    		// 用a轴上的索引不可行，因为需要读取body
			
    		int baseOffset = tSliceRecordOffset[tSliceLow];
    		int nRecord = tSliceRecordOffset[tSliceHigh + 1] - tSliceRecordOffset[tSliceLow];
    		
    		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
    		ByteBuffer bodyBuffer = ByteBuffer.allocate(nRecord * 34);
    		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);

			int nPointRead = tAxisPointChannel.read(pointBuffer, (long)baseOffset * 16);
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

    		
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		Collections.sort(result, tComparator);

//		System.out.println("[" + new Date() + "]: " + String.format("queryData: [%d %d] (%d %d %d %d) => %d", tMax-tMin, aMax-aMin, tMin, tMax, aMin, aMax, result.size()));

    	return result;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private static final int MAXPLAN = 2;
    private static final double IOSIZE_FACTOR = 20 * 1024; // SSD速度为 200MB/s 10000IOPS  这样算每个IO大约20KB
    
    private static class AverageResult {
    	long sum;
    	int cnt;
    	
    	//////////////
    	
    	long tAxisIOCount;
    	long tAxisIOBytes;
    	
    	long aAxisIOCount;
    	long aAxisIOBytes;
    	
    	//////////////
    	
    	int curPlan;
    	double ioCost[] = new double[MAXPLAN];
    	
    	void addIOCost(long nBytes)
    	{
    		// 若IO字节数太小，则按IO次数为1计算代价
    		// 若IO字节数太大，则把IO字节数换算成IO次数，计算代价
    		ioCost[curPlan] += Math.max(1.0, nBytes / IOSIZE_FACTOR);
    	}

    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////
    // 算法0：查询矩形的四个边
    
    private static void queryAverageSliceT(AverageResult result, boolean doRealQuery, int tSliceId, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = tSliceRecordOffset[tSliceId];
		int nRecord = tSliceRecordCount[tSliceId];
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++; // FIXME: 其实这里读取的不是Index文件，应分开统计
		result.tAxisIOBytes += nRecord * 16;
		
		ByteBuffer pointBuffer = ByteBuffer.allocateDirect(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisPointChannel.read(pointBuffer, (long)baseOffset * 16);
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
    
    
    private static void queryAverageSliceA(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
//    	for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
//    		queryAverageSliceT(result, doRealQuery, tSliceId, tMin, tMax, aMin, aMax);
//    	}
//    	if (true) return;
//    	System.out.println(String.format("(%d %d %d %d)", tMin, tMax, aMin, aMax));

    	int baseOffsetLow = blockOffsetTableAxisA[tSliceLow][aSliceLow];
		int nRecordLow = blockOffsetTableAxisA[tSliceHigh][aSliceLow] + blockCountTable[tSliceHigh][aSliceLow] - baseOffsetLow;

		
		int baseOffsetHigh = blockOffsetTableAxisA[tSliceLow][aSliceHigh];
		int nRecordHigh = blockOffsetTableAxisA[tSliceHigh][aSliceHigh] + blockCountTable[tSliceHigh][aSliceHigh] - baseOffsetHigh;

		
		
		result.addIOCost((long)nRecordLow * 8);
		result.addIOCost((long)nRecordHigh * 8);
		if (!doRealQuery) return;
		result.aAxisIOCount += 2;
		result.aAxisIOBytes += (nRecordLow + nRecordHigh) * 8;
		
		
		ByteBuffer lowBuffer = ByteBuffer.allocateDirect(nRecordLow * 8);
		lowBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisIndexChannel.read(lowBuffer, (long)baseOffsetLow * 8);
		lowBuffer.position(0);
		LongBuffer lowBufferL = lowBuffer.asLongBuffer();
		
		
		ByteBuffer highBuffer = ByteBuffer.allocateDirect(nRecordHigh * 8);
		highBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisIndexChannel.read(highBuffer, (long)baseOffsetHigh * 8);
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
				long prefixSum = lowBufferL.get(i);
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
				long prefixSum = highBufferL.get(i);
				long a = prefixSum - lastPrefixSum;
//				System.out.println(String.format("high t=%d a=%d", highBufferL.get(i * 2), a));
				lastPrefixSum = prefixSum;
				if (a <= aMax) {
					highSum = highBufferL.get(i);
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
    
    private static void queryAlgorithm0(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	if (tSliceLow == tSliceHigh) {
    		// 在同一个a块内，只能暴力
    		queryAverageSliceT(result, doRealQuery, tSliceLow, tMin, tMax, aMin, aMax);
    		
    	} else {
    		
    		queryAverageSliceT(result, doRealQuery, tSliceLow, tMin, tMax, aMin, aMax);
    		queryAverageSliceT(result, doRealQuery, tSliceHigh, tMin, tMax, aMin, aMax);
    		tSliceLow++;
    		tSliceHigh--;
    		if (tSliceLow <= tSliceHigh) {
    			queryAverageSliceA(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		}
    	}
    }
    
    
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法1：对t轴上的分块进行暴力查找
    
    private static void queryAlgorithm1(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = tSliceRecordOffset[tSliceLow];
		int nRecord = tSliceRecordOffset[tSliceHigh + 1] - tSliceRecordOffset[tSliceLow];
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++; // FIXME: 其实这里读取的不是Index文件，应分开统计
		result.tAxisIOBytes += nRecord * 16;
		
		ByteBuffer pointBuffer = ByteBuffer.allocateDirect(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisPointChannel.read(pointBuffer, (long)baseOffset * 16);
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
    
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////

    ///// 查询执行器：给定算法Id号，执行对应的查询
    private static void queryExecutor(AverageResult result, int planId, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	result.curPlan = planId;
    	result.ioCost[planId] = 0;
    	
    	switch (planId) {
    	case 0: queryAlgorithm0(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax); break;
    	case 1: queryAlgorithm1(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax); break;
    	default: assert false;
    	}
    }
    
    
    ////// 查询计划器：预估不同查询算法IO代价，选择IO代价最小的算法Id号返回
    private static int queryPlanner(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		queryExecutor(result, planId, false, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    	}
    	
    	double minIOCost = 1e100;
    	int optimalPlanId = -1;
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		if (result.ioCost[planId] < minIOCost) {
    			minIOCost = result.ioCost[planId];
    			optimalPlanId = planId;
    		}
    	}
    	
//    	optimalPlanId = 0;
    	return optimalPlanId;
    }
    

    
    ////////////////////////////////////////////////////////////////////////////////////////

    
    private static AtomicInteger totalAvgQuery = new AtomicInteger();
	private static AtomicLong tAxisIOCountTotal = new AtomicLong();
	private static AtomicLong tAxisIOBytesTotal = new AtomicLong();
	private static AtomicLong aAxisIOCountTotal = new AtomicLong();
	private static AtomicLong aAxisIOBytesTotal = new AtomicLong();
	private static DoubleAdder totalIOCost = new DoubleAdder();
	private static AtomicIntegerArray planCount = new AtomicIntegerArray(MAXPLAN);
	
	
    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	int aSliceLow = findSliceA(aMin);
    	int aSliceHigh = findSliceA(aMax);

    	AverageResult result = new AverageResult();
    	
//    	System.out.println(String.format("block: t[%d %d] a[%d %d]", tSliceLow, tSliceHigh, aSliceLow, aSliceHigh));  
    	try {

    		// 不同查询算法的IO代价可能不同
    		// 这里模仿数据库的查询计划器，先预估每种算法的IO代价，挑选最小的那个去执行
    		int optimalPlanId = queryPlanner(result, false, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		queryExecutor(result, optimalPlanId, true, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
	    	
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
    	System.out.println("[" + new Date() + "]: " + String.format("queryAverage: [t %d; a %d (%f)]; (%d %d %d %d) => cnt=%d; plan=%d [%f %f]; (t %d %d) (a %d %d)", tMax-tMin, aMax-aMin, (double)(aMax-aMin)/(globalMaxA - globalMinA), tMin, tMax, aMin, aMax, result.cnt, result.curPlan, result.ioCost[0], result.ioCost[1], result.tAxisIOCount, result.tAxisIOBytes, result.aAxisIOCount, result.aAxisIOBytes));
    	
    	
    	totalAvgQuery.incrementAndGet();
    	tAxisIOCountTotal.addAndGet(result.tAxisIOCount);
    	tAxisIOBytesTotal.addAndGet(result.tAxisIOBytes);
    	aAxisIOCountTotal.addAndGet(result.aAxisIOCount);
    	aAxisIOBytesTotal.addAndGet(result.aAxisIOBytes);
    	planCount.incrementAndGet(result.curPlan);
    	totalIOCost.add(result.ioCost[result.curPlan]);
    	
    	return result.cnt == 0 ? 0 : result.sum / result.cnt;
    }
    
    private static void atShutdown()
    {
    	System.out.println("[" + new Date() + "]: shutdown hook");
    	
    	
    	System.out.println(String.format("totalAvgQuery=%d", totalAvgQuery.get()));
    	System.out.println(String.format("tAxisIOCountTotal=%d", tAxisIOCountTotal.get()));
    	System.out.println(String.format("tAxisIOBytesTotal=%d", tAxisIOBytesTotal.get()));
    	System.out.println(String.format("aAxisIOCountTotal=%d", aAxisIOCountTotal.get()));
    	System.out.println(String.format("aAxisIOBytesTotal=%d", aAxisIOBytesTotal.get()));
    	
    	for (int i = 0; i < MAXPLAN; i++) {
    		System.out.println(String.format("planCount[%d]=%d", i, planCount.get(i)));
    	}
    	System.out.println(String.format("totalIOCost=%f", totalIOCost.sum()));
    }
}
