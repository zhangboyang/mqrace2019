package io.openmessaging;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

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
    
//    private static final String storagePath = "./";
    private static final String storagePath = "/alidata1/race2019/data/";
    
    private static final String tAxisPointFile = storagePath + "tAxis.point.data";
    private static final String tAxisBodyFile = storagePath + "tAxis.body.data";
    
    private static final String aAxisIndexFile = storagePath + "aAxis.index.data";
    private static final String tAxisIndexFile = storagePath + "tAxis.index.data";
    
    private static final RandomAccessFile tAxisPointData;
    private static final RandomAccessFile tAxisBodyData;
    private static final RandomAccessFile aAxisIndexData;
    private static final RandomAccessFile tAxisIndexData;
    
    private static final FileChannel tAxisPointChannel;
    private static final FileChannel tAxisBodyChannel;
    private static final FileChannel aAxisIndexChannel;
    private static final FileChannel tAxisIndexChannel;
    
    static {
    	RandomAccessFile tpFile, tbFile, aIndexFile, tIndexFile;
    	FileChannel tpChannel, tbChannel, aIndexChannel, tIndexChannel;
    	try {
			tpFile = new RandomAccessFile(tAxisPointFile, "rw");
			tpFile.setLength(0);
			tbFile = new RandomAccessFile(tAxisBodyFile, "rw");
			tbFile.setLength(0);
			aIndexFile = new RandomAccessFile(aAxisIndexFile, "rw");
			aIndexFile.setLength(0);
			tIndexFile = new RandomAccessFile(tAxisIndexFile, "rw");
			tIndexFile.setLength(0);
			
			tpChannel = FileChannel.open(Paths.get(tAxisPointFile));
			tbChannel = FileChannel.open(Paths.get(tAxisBodyFile));
			aIndexChannel = FileChannel.open(Paths.get(aAxisIndexFile));
			tIndexChannel = FileChannel.open(Paths.get(tAxisIndexFile));
			
		} catch (IOException e) {
			tpFile = null;
			tbFile = null;
			aIndexFile = null;
			tIndexFile = null;
			tpChannel = null;
			tbChannel = null;
			aIndexChannel = null;
			tIndexChannel = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisPointData = tpFile;
    	tAxisBodyData = tbFile;
    	aAxisIndexData = aIndexFile;
    	tAxisIndexData = tIndexFile;
        tAxisPointChannel = tpChannel;
        tAxisBodyChannel = tbChannel;
        aAxisIndexChannel = aIndexChannel;
        tAxisIndexChannel = tIndexChannel;
    }
    

    private static final int MAXMSG = 2100000000;
    private static final int N_TSLICE = 2000000;
    private static final int N_ASLICE = 60;
    
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
    
    
    
    
    private static ByteBuffer indexByteBuffer = null;
    private static void reserveIndexByteBuffer(int nBytes)
    {
    	if (indexByteBuffer == null || indexByteBuffer.capacity() < nBytes) {
    		indexByteBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		indexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    private static Message indexMsgBuffer[] = null;
    private static void reserveMsgBuffer(int nMsg)
    {
    	if (indexMsgBuffer == null || indexMsgBuffer.length < nMsg) {
    		indexMsgBuffer = new Message[nextSize(nMsg)];
    		for (int i = 0; i < indexMsgBuffer.length; i++) {
    			indexMsgBuffer[i] = new Message(0, 0, null);
    		}
    	}
    }
    
    
    private static void buildIndexAxisT() throws IOException
    {
    	System.out.println("[" + new Date() + "]: build index for t-axis");
    	
    	tAxisPointData.seek(0);
    	
    	long prefixSum[] = new long[N_ASLICE];
    	
    	for (int tSliceId = 0; tSliceId < tSliceCount; tSliceId++) {
    		int nRecord = tSliceRecordCount[tSliceId];
    		
    		reserveIndexByteBuffer(nRecord * 16);
    		reserveMsgBuffer(nRecord);
    		
//    		System.out.println(String.format("%d %d %d", nRecord, tAxisPointData.getFilePointer(), (long)tSliceRecordOffset[tSliceId] * 16));
    		assert tAxisPointData.getFilePointer() == (long)tSliceRecordOffset[tSliceId] * 16;
    		tAxisPointData.readFully(indexByteBuffer.array(), 0, nRecord * 16);
    		
    		System.arraycopy(prefixSum, 0, blockPrefixSumBaseTable[tSliceId], 0, N_ASLICE);
    		
    		indexByteBuffer.position(0);
    		LongBuffer indexLongBuffer = indexByteBuffer.asLongBuffer();
    		
    		for (int i = 0; i < nRecord; i++) {
    			long t = indexLongBuffer.get(i * 2);
    			long a = indexLongBuffer.get(i * 2 + 1);
    			indexMsgBuffer[i].setT(t);
    			indexMsgBuffer[i].setA(a);
    		}
    		
    		// t块内部按a排序
    		Arrays.sort(indexMsgBuffer, 0, nRecord, aComparator);
    		
    		// 计算每小块内记录数量
    		int aSliceId = 0;
    		for (int i = 0; i < nRecord; i++) {
    			Message msg = indexMsgBuffer[i];
    			long a = msg.getA();
    			
    			while (aSliceId < N_ASLICE && a >= aSlicePivot[aSliceId + 1]) aSliceId++;
    			assert aSliceId == findSliceA(a);
    			
    			blockCountTable[tSliceId][aSliceId]++;
    		}
    		
    		// 每小块内再按t排序，并计算前缀和
    		int recordOffset = 0;
    		for (aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
    			int recordCount = blockCountTable[tSliceId][aSliceId];
    			
    			Arrays.sort(indexMsgBuffer, recordOffset, recordOffset + recordCount, tComparator);
    			
    			for (int i = recordOffset; i < recordOffset + recordCount; i++) {
    				Message msg = indexMsgBuffer[i];
    				long l = msg.getT();
    				long a = msg.getA();
    				prefixSum[aSliceId] += a;
    				indexLongBuffer.put(i * 2, l);
        			indexLongBuffer.put(i * 2 + 1, prefixSum[aSliceId]);
    			}
    			
    			recordOffset += recordCount;
    		}
    		
    		tAxisIndexData.write(indexByteBuffer.array(), 0, nRecord * 16);
    	}
    	
    	System.out.println("[" + new Date() + "]: t-axis index finished");
    }
    
    
    
    
    
    
    
    private static final int BATCHSIZE = 3000;
    
    
    private static void buildIndexForRangeAxisA(int tSliceFrom, int tSliceTo) throws IOException
    {
//    	System.out.println("[" + new Date() + "]: " + String.format("from=%d to=%d", tSliceFrom, tSliceTo));
    	
    	int nRecord = tSliceRecordOffset[tSliceTo + 1] - tSliceRecordOffset[tSliceFrom];
    	reserveIndexByteBuffer(nRecord * 16);
    	reserveMsgBuffer(nRecord);
    	
    	assert tAxisPointData.getFilePointer() == (long)tSliceRecordOffset[tSliceFrom] * 16;
    	tAxisPointData.readFully(indexByteBuffer.array(), 0, nRecord * 16);
    	indexByteBuffer.position(0);
		LongBuffer indexLongBuffer = indexByteBuffer.asLongBuffer();
		
		for (int i = 0; i < nRecord; i++) {
			long t = indexLongBuffer.get();
			long a = indexLongBuffer.get();
			indexMsgBuffer[i].setT(t);
			indexMsgBuffer[i].setA(a);
		}
		
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
			// t块内部按a排序
			Arrays.sort(indexMsgBuffer, tSliceRecordOffset[tSliceId] - tSliceRecordOffset[tSliceFrom], tSliceRecordOffset[tSliceId + 1] - tSliceRecordOffset[tSliceFrom], aComparator);
			
			for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
				int msgCnt = blockCountTable[tSliceId][aSliceId];
				
				int putBase = bufferBase[aSliceId] + blockOffsetTableAxisA[tSliceId][aSliceId] - blockOffsetTableAxisA[tSliceFrom][aSliceId];
				for (int i = 0; i < msgCnt; i++) { 
					Message msg = indexMsgBuffer[msgPtr++];
					indexLongBuffer.put((putBase + i) * 2, msg.getT());
					indexLongBuffer.put((putBase + i) * 2 + 1, msg.getA());
				}
			}
		}
		assert msgPtr == nRecord;
		
		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
			aAxisIndexData.seek((long)blockOffsetTableAxisA[tSliceFrom][aSliceId] * 16);
			aAxisIndexData.write(indexByteBuffer.array(), bufferBase[aSliceId] * 16, sliceRecordCount[aSliceId] * 16);
		}
    }
    
    private static void buildIndexAxisA() throws IOException
    {
    	System.out.println("[" + new Date() + "]: build index for a-axis");
    	
    	tAxisPointData.seek(0);
    	aAxisIndexData.setLength(tAxisPointData.length()); // FIXME: 是否会造成文件在磁盘上存储不连续？
    	
    	for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId += BATCHSIZE) {
    		buildIndexForRangeAxisA(tSliceId, Math.min(tSliceId + BATCHSIZE, tSliceCount) - 1);
    	}
    	
    	System.out.println("[" + new Date() + "]: a-axis index finished");
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
    
    
   
    
    
    
    
    
    
    
    
    
    
    private static long tValueBuffer[] = new long[1048576];
    private static int tValueBufferPtr = 0;
    private static void putTValue(long tValue)
    {
    	tValueBuffer[tValueBufferPtr++] = tValue;
    	if (tValueBufferPtr == tValueBuffer.length) {
    		long newBuffer[] = new long[tValueBuffer.length * 2];
    		System.arraycopy(tValueBuffer, 0, newBuffer, 0, tValueBuffer.length);
    		tValueBuffer = newBuffer;
    	}
    }

    private static void calcSliceRecordCount(long exclusiveT) throws IOException
    {
		int nInclusive;
		for (nInclusive = 0; nInclusive < tValueBufferPtr; nInclusive++) {
			long curT = tValueBuffer[nInclusive];
			if (curT == exclusiveT) {
				break;
			}
		}

		tSliceRecordCount[tSliceCount - 1] = nInclusive;
		
		System.arraycopy(tValueBuffer, nInclusive, tValueBuffer, 0, tValueBufferPtr - nInclusive);
		tValueBufferPtr -= nInclusive;
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
    	long curA = buffer.getLong(offset + 8);
    	globalMinA = Math.min(globalMinA, curA);
    	globalMaxA = Math.max(globalMaxA, curA);
    	
		long curT = buffer.getLong(offset);
		
		if (insCount % TSLICE_INTERVAL == 0) {
			if (insCount > 0) {
				calcSliceRecordCount(curT);
			}
			tSlicePivot[tSliceCount++] = curT;
		}
		
		putTValue(curT);
		tAxisPointStream.write(buffer.array(), offset, 16);
		tAxisBodyStream.write(buffer.array(), offset + 16, 34);
		
    	insCount++;
    	if (insCount % 1000000 == 0) {
			System.out.println("[" + new Date() + "]: " + String.format("ins %d: t=%d a=%d", insCount, curT, curA));
		}
    }
    private static void finishInsertMessage() throws IOException
    {
    	calcSliceRecordCount(Long.MAX_VALUE);
    	tSlicePivot[tSliceCount] = Long.MAX_VALUE;
    	assert tValueBufferPtr == 0;
    	tValueBuffer = null;
    	tAxisPointStream.close();
    	tAxisPointStream = null;
    	tAxisBodyStream.close();
    	tAxisBodyStream = null;
    }
    



    private static final int MAX_MSGBUF = 1000;
    private static void externalMergeSort() throws IOException
    {
    	System.out.println("[" + new Date().toString() + "]: merge-sort begin!");
    	

		int nThread = putThreadCount.get();
		
		ByteBuffer queueHead = ByteBuffer.allocate(64 * nThread); // 还是按64字节对齐一下吧
		queueHead.order(ByteOrder.LITTLE_ENDIAN);
		int readCount[] = new int[nThread]; 
		int recordCount[] = new int[nThread];
		
		for (int i = 0; i < nThread; i++) {
			recordCount[i] = putTLD[i].outputCount;
			readCount[i] = 0;
			putTLD[i].bufferedInputStream = new BufferedInputStream(new FileInputStream(putTLD[i].dataFileName));
			putTLD[i].bufferedInputStream.read(queueHead.array(), i * 64, 50);
		}
		
		beginInsertMessage();
		
		while (true) {
			
			long minValue = Long.MAX_VALUE;
			int minPos = -1;
			for (int i = 0; i < nThread; i++) {
				if (readCount[i] < recordCount[i]) {
					long curValue = queueHead.getLong(i * 64);
					if (curValue <= minValue) {
						minValue = curValue;
						minPos = i;
					}
				}
			}
			
			if (minPos == -1) {
				break;
			}
			
			insertMessage(queueHead, minPos * 64);
			readCount[minPos]++;
			putTLD[minPos].bufferedInputStream.read(queueHead.array(), minPos * 64, 50);
		}
		
		finishInsertMessage();
		
		for (int i = 0; i < nThread; i++) {
			putTLD[i].bufferedInputStream.close();
			putTLD[i].bufferedInputStream = null;
		}
		System.out.println("[" + new Date().toString() + "]: merge-sort completed!");
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
    	
    	// 计算a轴上的分割点
    	for (int i = 0; i < N_ASLICE; i++) {
    		aSlicePivot[i] = globalMinA + (globalMaxA - globalMinA) / N_ASLICE * i;
    	}
    	aSlicePivot[N_ASLICE] = Long.MAX_VALUE;
    	
    	// 建立T轴上的索引
    	buildIndexAxisT();
    	
    	// 建立内存内偏移表
    	buildOffsetTable();
    	
    	// 建立A轴上的索引
    	buildIndexAxisA();
    	
    	// 关闭用于写入的文件
    	tAxisPointData.close();
    	tAxisBodyData.close();
    	aAxisIndexData.close();
    	tAxisIndexData.close();
    	
    	// 释放临时内存
    	indexByteBuffer = null;
    	indexMsgBuffer = null;
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private static final int MAXTHREAD = 100;

    private static class PutThreadLocalData {
    	BufferedOutputStream bufferedOutputStream;
    	BufferedInputStream bufferedInputStream;
    	
    	ByteBuffer msgData;
    	int outputCount = 0;
    	
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
    		pd.msgData.position(0);
    		pd.msgData.putLong(message.getT());
    		pd.msgData.putLong(message.getA());
    		pd.msgData.put(message.getBody());

    		pd.outputCount++;
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
						externalMergeSort();
						buildIndex();
						
					} catch (Exception e) {
						e.printStackTrace();
						state = -1;
						System.exit(-1);
					}
    				
    				
    				System.out.println(String.format("insCount=%d", insCount));
    				System.out.println(String.format("globalMinA=%d", globalMinA));
    				System.out.println(String.format("globalMaxA=%d", globalMaxA));
    				
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
		
//		Collections.sort(result, tComparator);

//		System.out.println("[" + new Date() + "]: " + String.format("queryData: [%d %d] (%d %d %d %d) => %d", tMax-tMin, aMax-aMin, tMin, tMax, aMin, aMax, result.size()));

    	return result;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private static final int MAXPLAN = 2;
    private static final double IOSIZE_FACTOR = 20 * 1024; // SSD速度为 200MB/s 10000IOPS  这样算每个IO大约20KB
    
    private static class AverageResult {
    	long sum = 0;
    	int cnt = 0;
    	
    	long tAxisIOCount = 0;
    	long tAxisIOBytes = 0;
    	
    	long aAxisIOCount = 0;
    	long aAxisIOBytes = 0;
    	
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
    
    private static void queryAverageSliceA(AverageResult result, boolean doRealQuery, int aSliceId, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = blockOffsetTableAxisA[tSliceLow][aSliceId];
		int nRecord = blockOffsetTableAxisA[tSliceHigh + 1][aSliceId] - baseOffset;
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.aAxisIOCount++;
		result.aAxisIOBytes += nRecord * 16;
		
		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisIndexChannel.read(pointBuffer, (long)baseOffset * 16);
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
    
    
    private static void queryAverageAxisT(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
//    	for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
//    		queryAverageAxisT(result, tSliceId, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
//    	}
//    	if (true) return;
//    	System.out.println(String.format("(%d %d %d %d)", tMin, tMax, aMin, aMax));
    			
    	int baseOffsetLow = blockOffsetTableAxisT[tSliceLow][aSliceLow];
		int nRecordLow = blockOffsetTableAxisT[tSliceLow][aSliceHigh + 1] - baseOffsetLow;

		
		int baseOffsetHigh = blockOffsetTableAxisT[tSliceHigh][aSliceLow];
		int nRecordHigh = blockOffsetTableAxisT[tSliceHigh][aSliceHigh + 1] - baseOffsetHigh;

		result.addIOCost((long)nRecordLow * 16);
		result.addIOCost((long)nRecordHigh * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++;
		result.tAxisIOBytes += nRecordLow * 16;
		result.tAxisIOCount++;
		result.tAxisIOBytes += nRecordHigh * 16;
		
		ByteBuffer lowBuffer = ByteBuffer.allocate(nRecordLow * 16);
		lowBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisIndexChannel.read(lowBuffer, (long)baseOffsetLow * 16);
		lowBuffer.position(0);
		LongBuffer lowBufferL = lowBuffer.asLongBuffer();
		
		
		ByteBuffer highBuffer = ByteBuffer.allocate(nRecordHigh * 16);
		highBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisIndexChannel.read(highBuffer, (long)baseOffsetHigh * 16);
		highBuffer.position(0);
		LongBuffer highBufferL = highBuffer.asLongBuffer();
		
		int lowOffset = 0;
		int highOffset = 0;
		for (int aSliceId = aSliceLow; aSliceId <= aSliceHigh; aSliceId++) {
			
			int lowCount = blockCountTable[tSliceLow][aSliceId];
			int highCount = blockCountTable[tSliceHigh][aSliceId];
			
			long lowSum = blockPrefixSumBaseTable[tSliceLow][aSliceId];
			int lowPtr = -1;
			for (int i = lowOffset; i < lowOffset + lowCount; i++) {
				long t = lowBufferL.get(i * 2);
//				System.out.println(String.format("low t=%d prefixSum=%d", lowBufferL.get(i * 2), lowBufferL.get(i * 2 + 1)));
				if (t < tMin) {
					lowSum = lowBufferL.get(i * 2 + 1);
					lowPtr = i - lowOffset;
				} else {
					break;
				}
			}
			lowPtr++;
			
			long highSum = blockPrefixSumBaseTable[tSliceHigh][aSliceId];
			int highPtr = -1;
			for (int i = highOffset; i < highOffset + highCount; i++) {
				long t = highBufferL.get(i * 2);
//				System.out.println(String.format("high t=%d prefixSum=%d", highBufferL.get(i * 2), highBufferL.get(i * 2 + 1)));
				if (t <= tMax) {
					highSum = highBufferL.get(i * 2 + 1);
					highPtr = i - highOffset;
				}
			}
			
			
			int globalLowPtr = blockOffsetTableAxisA[tSliceLow][aSliceId] + lowPtr;
			int globalHighPtr = blockOffsetTableAxisA[tSliceHigh][aSliceId] + highPtr;
			
			long sum = 0;
			int cnt = 0;
			if (globalHighPtr >= globalLowPtr) {
				sum = highSum - lowSum;
				cnt = globalHighPtr - globalLowPtr + 1;
			}
			
			result.sum += sum;
			result.cnt += cnt;
		
//			AverageResult referenceResult = new AverageResult();
//			queryAverageSliceA(referenceResult, aSliceId, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
//			System.out.println(String.format("aSliceId=%d ; tSliceLow=%d tSliceHigh=%d ; lowPtr=%d  highPtr=%d", aSliceId, tSliceLow, tSliceHigh, lowPtr, highPtr));
//			System.out.println(String.format("cnt=%d sum=%d", cnt, sum));
//			System.out.println(String.format("ref: cnt=%d sum=%d", referenceResult.cnt, referenceResult.sum));
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
    	if (aSliceLow == aSliceHigh) {
    		// 在同一个a块内，只能暴力
    		queryAverageSliceA(result, doRealQuery, aSliceLow, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    		
    	} else {
    		
    		queryAverageSliceA(result, doRealQuery, aSliceLow, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    		queryAverageSliceA(result, doRealQuery, aSliceHigh, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    		aSliceLow++;
    		aSliceHigh--;
    		if (aSliceLow <= aSliceHigh) {
    			queryAverageAxisT(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
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
		
		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
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
    	assert optimalPlanId >= 0;
    	
//    	optimalPlanId = 1;
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
