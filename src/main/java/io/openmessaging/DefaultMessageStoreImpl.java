package io.openmessaging;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

	
	
	
	private static class My2DIntArray {
		final int array[];
		final int width;
		final int height;
		
		public My2DIntArray(int _height, int _width) { width = _width; height = _height; array = new int[width * height]; }
		int get(int x, int y) { assert 0 <= x && x < height && 0 <= y && y < width; return array[x * width + y]; }
		void set(int x, int y, int value) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y] = value; }
		void inc(int x, int y) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y]++; }
		void add(int x, int y, int value) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y] += value; }
	}
	private static class My2DLongArray {
		final long array[];
		final int width;
		final int height;
		
		public My2DLongArray(int _height, int _width) { width = _width; height = _height; array = new long[width * height]; }
		long get(int x, int y) { assert 0 <= x && x < height && 0 <= y && y < width; return array[x * width + y]; }
		void set(int x, int y, long value) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y] = value; }
		void inc(int x, int y) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y]++; }
		void add(int x, int y, long value) { assert 0 <= x && x < height && 0 <= y && y < width; array[x * width + y] += value; }
	}
	
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
    
    
    private static class ValueCompressor {
    	// 数值压缩器：用类似UTF-8的变长编码来压缩一个64位整数（这是通用的算法！！！）
    	// 压缩后的数据最长可能需要9字节
    	
    	// 编码说明：
    	//    0xxxxxxx   1字节-7bit
    	//    10xxxxxx xxxxxxxx   2字节-14bit
    	//    110xxxxx xxxxxxxx xxxxxxxx   3字节-21bit
    	//    1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx   4字节-28bit
    	//    11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   5字节-35bit
    	//    111110xx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   6字节-42bit
    	//    1111110x xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx  7字节-49bit
    	//    11111110 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   8字节-56bit
    	//    11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx  9字节-64bit
    	
    	
    	public static void putToBuffer(ByteBuffer buffer, long value)
    	{
    		assert buffer.order() == ByteOrder.LITTLE_ENDIAN;

    		if ((value & 0x7fL) == value) {
    			buffer.put((byte)(value));
    		} else if ((value & 0x3fffL) == value) {
    			buffer.put((byte)((value >>> 8) | 0x80));
    			buffer.put((byte)(value));
    		} else if ((value & 0x1fffffL) == value) {
    			buffer.put((byte)((value >>> 16) | 0xc0));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else if ((value & 0xfffffffL) == value) {
    			buffer.put((byte)((value >>> 24) | 0xe0));
    			buffer.put((byte)(value >>> 16));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else if ((value & 0x7ffffffffL) == value) {
    			buffer.put((byte)((value >>> 32) | 0xf0));
    			buffer.put((byte)(value >>> 24));
    			buffer.put((byte)(value >>> 16));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else if ((value & 0x3ffffffffffL) == value) {
    			buffer.put((byte)((value >>> 40) | 0xf8));
    			buffer.put((byte)(value >>> 32));
    			buffer.put((byte)(value >>> 24));
    			buffer.put((byte)(value >>> 16));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else if ((value & 0x1ffffffffffffL) == value) {
    			buffer.put((byte)((value >>> 48) | 0xfc));
    			buffer.put((byte)(value >>> 40));
    			buffer.put((byte)(value >>> 32));
    			buffer.put((byte)(value >>> 24));
    			buffer.put((byte)(value >>> 16));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else if ((value & 0xffffffffffffffL) == value) {
    			buffer.put((byte)0xfe);
    			buffer.put((byte)(value >>> 48));
    			buffer.put((byte)(value >>> 40));
    			buffer.put((byte)(value >>> 32));
    			buffer.put((byte)(value >>> 24));
    			buffer.put((byte)(value >>> 16));
    			buffer.put((byte)(value >>> 8));
    			buffer.put((byte)(value));
    		} else {
    			buffer.put((byte)0xff);
    			buffer.putLong(value);
    		}
    	}
    	
    	public static int getLengthByValue(long value)
    	{
    		if ((value & 0x7fL) == value) {
    			return 1;
    		} else if ((value & 0x3fffL) == value) {
    			return 2;
    		} else if ((value & 0x1fffffL) == value) {
    			return 3;
    		} else if ((value & 0xfffffffL) == value) {
    			return 4;
    		} else if ((value & 0x7ffffffffL) == value) {
    			return 5;
    		} else if ((value & 0x3ffffffffffL) == value) {
    			return 6;
    		} else if ((value & 0x1ffffffffffffL) == value) {
    			return 7;
    		} else if ((value & 0xffffffffffffffL) == value) {
    			return 8;
    		} else {
    			return 9;
    		}
		}
    	
    	public static long getFromBuffer(ByteBuffer buffer)
    	{
    		assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
    		long value;
    		long firstbyte = ((int)buffer.get()) & 0xff;
    		if (firstbyte < 0x80) {
    			value = firstbyte;
    		} else if (firstbyte < 0xc0) {
    			value = (firstbyte & 0x3f) << 8; 
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xe0) {
    			value = (firstbyte & 0x1f) << 16; 
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xf0) {
    			value = (firstbyte & 0x0f) << 24; 
    			value |= ((long)buffer.get() & 0xff) << 16;
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xf8) {
    			value = (firstbyte & 0x07) << 32; 
    			value |= ((long)buffer.get() & 0xff) << 24;
    			value |= ((long)buffer.get() & 0xff) << 16;
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xfc) {
    			value = (firstbyte & 0x03) << 40; 
    			value |= ((long)buffer.get() & 0xff) << 32;
    			value |= ((long)buffer.get() & 0xff) << 24;
    			value |= ((long)buffer.get() & 0xff) << 16;
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xfe) {
    			value = (firstbyte & 0x01) << 48; 
    			value |= ((long)buffer.get() & 0xff) << 40;
    			value |= ((long)buffer.get() & 0xff) << 32;
    			value |= ((long)buffer.get() & 0xff) << 24;
    			value |= ((long)buffer.get() & 0xff) << 16;
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else if (firstbyte < 0xff) {
    			value = ((long)buffer.get() & 0xff) << 48;
    			value |= ((long)buffer.get() & 0xff) << 40;
    			value |= ((long)buffer.get() & 0xff) << 32;
    			value |= ((long)buffer.get() & 0xff) << 24;
    			value |= ((long)buffer.get() & 0xff) << 16;
    			value |= ((long)buffer.get() & 0xff) << 8;
    			value |= ((long)buffer.get() & 0xff);
    		} else {
    			value = buffer.getLong();
    		}
    		
    		return value;
    	}
    }
    
    
    
    
    
    
    
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

		byte zeros[] = new byte[4096];
		RandomAccessFile fp = new RandomAccessFile(fileName, "rw");
		// 理论上，用fallocate()系统调用，可以不用写数据而达到预留磁盘空间的目的，但Java8不支持
		// 所以这里使用向文件填0的方法
//		fp.setLength(0);
//		while (nBytes > 0) {
//			int nWrite = (int) Math.min(nBytes, zeros.length);
//			fp.write(zeros, 0, nWrite);
//			nBytes -= nWrite;
//		}
		fp.setLength(nBytes);//FIXME
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
    	if (bytes != null) {
	    	for (int j = 0; j < bytes.length; j++) {
	            int v = bytes[j] & 0xFF;
	            s.append(HEX_ARRAY[v >>> 4]);
	            s.append(HEX_ARRAY[v & 0x0F]);
	        }
    	} else {
    		s.append("NULL");
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
    private static final String tAxisBodyFile = storagePath + "tAxis.bodyref.data";
    private static final String tAxisCompressedPointFile = storagePath + "tAxis.zp.data";
    private static final String aAxisIndexFile = storagePath + "aAxis.prefixsum.data";
    
    private static final RandomAccessFile tAxisPointData;
    private static final RandomAccessFile tAxisBodyData;
    private static final RandomAccessFile tAxisCompressedPointData;
    private static final RandomAccessFile aAxisIndexData;
    
    private static final FileChannel tAxisPointChannel;
    private static final FileChannel tAxisBodyChannel;
    private static final FileChannel tAxisCompressedPointChannel;
    private static final FileChannel aAxisIndexChannel;

    static {
    	RandomAccessFile tpFile, tbFile, tzpFile, aIndexFile, azp2File, azp3File;
    	FileChannel tpChannel, tbChannel, tzpChannel, aIndexChannel, azp2Channel, azp3Channel;
    	try {
			tpFile = new RandomAccessFile(tAxisPointFile, "rw");
			tpFile.setLength(0);
			tbFile = new RandomAccessFile(tAxisBodyFile, "rw");
			tbFile.setLength(0);
			tzpFile = new RandomAccessFile(tAxisCompressedPointFile, "rw");
			tzpFile.setLength(0);
			aIndexFile = new RandomAccessFile(aAxisIndexFile, "rw");
			aIndexFile.setLength(0);
			
			tpChannel = FileChannel.open(Paths.get(tAxisPointFile));
			tbChannel = FileChannel.open(Paths.get(tAxisBodyFile));
			tzpChannel = FileChannel.open(Paths.get(tAxisCompressedPointFile));
			aIndexChannel = FileChannel.open(Paths.get(aAxisIndexFile));
			
		} catch (IOException e) {
			tpFile = null;
			tbFile = null;
			tzpFile = null;
			aIndexFile = null;
			azp2File = null;
			azp3File = null;
			tpChannel = null;
			tbChannel = null;
			tzpChannel = null;
			aIndexChannel = null;
			azp2Channel = null;
			azp3Channel = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisPointData = tpFile;
    	tAxisBodyData = tbFile;
    	tAxisCompressedPointData = tzpFile;
    	aAxisIndexData = aIndexFile;
        tAxisPointChannel = tpChannel;
        tAxisBodyChannel = tbChannel;
        tAxisCompressedPointChannel = tzpChannel;
        aAxisIndexChannel = aIndexChannel;
    }
    
    
    
    private static final int MAXTHREAD = 100;
    
    private static final int MAXMSG = 2100000000;
    private static final int N_TSLICE = 3000000;
    private static final int N_ASLICE = 40;
    
    private static final int N_ASLICE2 = 8;
    private static final int N_ASLICE3 = N_ASLICE2 + 1;
    
    private static final int TSLICE_INTERVAL = MAXMSG / N_TSLICE;
    
    private static int tSliceCount = 0;
    private static final long tSlicePivot[] = new long[N_TSLICE + 1];
    private static final int tSliceRecordCount[] = new int[N_TSLICE + 1];
    private static final int tSliceRecordOffset[] = new int[N_TSLICE + 1];
    
    private static final long tSliceCompressedPointByteOffset[] = new long[N_TSLICE + 1]; // FIXME: 改成二维？
    
    private static final long aSlicePivot[] = new long[N_ASLICE + 1];
    
    private static final long aSlice2Pivot[] = new long[N_ASLICE2 + 1];
    private static final My2DLongArray aAxisCompressedPoint2BaseT = new My2DLongArray(N_TSLICE + 1, N_ASLICE2);
    private static final My2DLongArray aAxisCompressedPoint2ByteOffset = new My2DLongArray(N_TSLICE + 1, N_ASLICE2);
    
    private static final long aSlice3Pivot[] = new long[N_ASLICE3 + 1];
    private static final My2DLongArray aAxisCompressedPoint3BaseT = new My2DLongArray(N_TSLICE + 1, N_ASLICE3);
    private static final My2DLongArray aAxisCompressedPoint3ByteOffset = new My2DLongArray(N_TSLICE + 1, N_ASLICE3);
    
    
    private static final My2DIntArray blockOffsetTableAxisT = new My2DIntArray(N_TSLICE + 1, N_ASLICE + 1);
    private static final My2DIntArray blockOffsetTableAxisA = new My2DIntArray(N_TSLICE + 1, N_ASLICE + 1);
//    private static final My2DLongArray blockPrefixSumBaseTable = new My2DLongArray(N_TSLICE, N_ASLICE);
    
    private static int insCount = 0;
    
    private static int globalTotalRecords = 0;
    private static long globalMaxA = Long.MIN_VALUE;
    private static long globalMinA = Long.MAX_VALUE;
    
    private static final long zpByteOffset[] = new long[MAXTHREAD];
    private static final int zpRecordOffset[] = new int[MAXTHREAD];
    private static final long zpLastT[] = new long[MAXTHREAD];

    
    
    
    
    
    private static final BufferedOutputStream aAxisCompressedPoint2Data[] = new BufferedOutputStream[N_ASLICE2];
    private static final FileChannel aAxisCompressedPoint2Channel[] = new FileChannel[N_ASLICE2];
    private static long aAxisCompressedPoint2OutputBytes = 0;
    
    private static final BufferedOutputStream aAxisCompressedPoint3Data[] = new BufferedOutputStream[N_ASLICE3];
    private static final FileChannel aAxisCompressedPoint3Channel[] = new FileChannel[N_ASLICE3];
    private static long aAxisCompressedPoint3OutputBytes = 0;
    
    private static final int BUFSZ = 1024 * 1024 * 32;
    static {
    	try {
	    	for (int i = 0; i < N_ASLICE2; i++) {
	    		String fn = storagePath + String.format("aAxis.zp2.%04d.data", i);
	    		aAxisCompressedPoint2Data[i] = new BufferedOutputStream(new FileOutputStream(fn), BUFSZ);
	    		aAxisCompressedPoint2Channel[i] = FileChannel.open(Paths.get(fn));
	    	}
	    	for (int i = 0; i < N_ASLICE3; i++) {
	    		String fn = storagePath + String.format("aAxis.zp3.%04d.data", i);
	    		aAxisCompressedPoint3Data[i] = new BufferedOutputStream(new FileOutputStream(fn), BUFSZ);
	    		aAxisCompressedPoint3Channel[i] = FileChannel.open(Paths.get(fn));
	    	}
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(-1);
    	}
    }
    
    
    
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
    private static int findSliceA2(long aValue)
    {
		int l = 0, r = N_ASLICE2;
		while (r - l > 1) {
			int m = (l + r) / 2;
			if (aValue >= aSlice2Pivot[m]) {
				l = m;
			} else {
				r = m;
			}
		}
		assert aSlice2Pivot[l] <= aValue && aValue < aSlice2Pivot[l + 1];
		return l;
    }
    private static int findSliceA3(long aValue)
    {
		int l = 0, r = N_ASLICE3;
		while (r - l > 1) {
			int m = (l + r) / 2;
			if (aValue >= aSlice3Pivot[m]) {
				l = m;
			} else {
				r = m;
			}
		}
		assert aSlice3Pivot[l] <= aValue && aValue < aSlice3Pivot[l + 1];
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
    	reserveIndexWriteBuffer((nRecord + BATCHSIZE * N_ASLICE) * 8);
    	
    	assert tAxisPointData.getFilePointer() == (long)tSliceRecordOffset[tSliceFrom] * 16;
    	tAxisPointData.readFully(indexReadBuffer.array(), 0, nRecord * 16);
    	indexReadBuffer.position(0);
		LongBuffer indexReadBufferL = indexReadBuffer.asLongBuffer();
		
		indexWriteBuffer.position(0);
		LongBuffer indexWriteBufferL = indexWriteBuffer.asLongBuffer();

		
		// 造a轴前缀和索引
		int sliceRecordCount[] = new int[N_ASLICE];
		int bufferBase[] = new int[N_ASLICE];
		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
			sliceRecordCount[aSliceId] = blockOffsetTableAxisA.get(tSliceTo + 1, aSliceId) - blockOffsetTableAxisA.get(tSliceFrom, aSliceId);
			if (aSliceId > 0) {
				bufferBase[aSliceId] = bufferBase[aSliceId - 1] + sliceRecordCount[aSliceId - 1];
			}
		}
		
		int msgPtr = 0;
		for (int tSliceId = tSliceFrom; tSliceId <= tSliceTo; tSliceId++) {
			
			long prefixSum = 0;
			
			for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
				int msgCnt = blockOffsetTableAxisA.get(tSliceId + 1, aSliceId) - blockOffsetTableAxisA.get(tSliceId, aSliceId) - 1;
				
				int putBase = bufferBase[aSliceId] + blockOffsetTableAxisA.get(tSliceId, aSliceId) - blockOffsetTableAxisA.get(tSliceFrom, aSliceId);

				// 存储prefixSumBase
				indexWriteBufferL.put(putBase + msgCnt, prefixSum);
				
				// 存储各个prefixSum
				for (int i = putBase; i < putBase + msgCnt; i++) {
					
					long curA = indexReadBufferL.get((msgPtr++ * 2) + 1);
					prefixSum += curA;
					
					indexWriteBufferL.put(i, prefixSum);
				}
			}
		}
		assert msgPtr == nRecord;
		
		for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
			aAxisIndexData.seek((long)blockOffsetTableAxisA.get(tSliceFrom, aSliceId) * 8);
			aAxisIndexData.write(indexWriteBuffer.array(), bufferBase[aSliceId] * 8, sliceRecordCount[aSliceId] * 8);
		}
    }
    
    private static void buildIndexAxisA() throws IOException
    {
    	System.out.println("[" + new Date() + "]: build index for a-axis");
    	
    	tAxisPointData.seek(0);
    	reserveDiskSpace(aAxisIndexFile, (long)insCount * 8);
    	
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
    			int t = blockOffsetTableAxisT.get(tSliceId, aSliceId);
    			blockOffsetTableAxisT.set(tSliceId, aSliceId, offset);
    			offset += t;
    		}
    	}
    	assert offset == insCount;
    	
    	offset = 0;
    	for (int aSliceId = 0; aSliceId <= N_ASLICE; aSliceId++) {
    		for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
    			int t = blockOffsetTableAxisA.get(tSliceId, aSliceId);
    			if (aSliceId < N_ASLICE && tSliceId < tSliceCount) {
    				t++;  // 每个块第一个记录是该块的prefixSumBase
    			}
    			blockOffsetTableAxisA.set(tSliceId, aSliceId, offset);
    			offset += t;
    		}
    	}
    	assert offset == insCount + N_ASLICE * tSliceCount;
    	
    	
    	
    	// 造a轴压缩点偏移表
    	for (int aSlice2Id = 0; aSlice2Id < N_ASLICE2; aSlice2Id++) {
    		long offsetL = 0;
    		for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
    			long t = aAxisCompressedPoint2ByteOffset.get(tSliceId, aSlice2Id);
    			aAxisCompressedPoint2ByteOffset.set(tSliceId, aSlice2Id, offsetL);
    			offsetL += t;
    		}
    	}
    	
    	
    	// 造3号a轴压缩点偏移表
    	for (int aSlice3Id = 0; aSlice3Id < N_ASLICE3; aSlice3Id++) {
    		long offsetL = 0;
    		for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
    			long t = aAxisCompressedPoint3ByteOffset.get(tSliceId, aSlice3Id);
    			aAxisCompressedPoint3ByteOffset.set(tSliceId, aSlice3Id, offsetL);
    			offsetL += t;
    		}
    	}
    	
    	
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
    	tAxisCompressedPointData.close();
    	aAxisIndexData.close();
    	
    	// 释放内存
    	writeBuffer = null;
    	writeBuffer2 = null;
    }
    
    
    
    
    
    
    
    private static class MessageWithMetadata extends Message {
    	int threadId;
    	int recordOffset;
		long byteOffset;
		
		int aSliceId2;
		int aSliceId3;
    	
		public MessageWithMetadata(long a, long t, byte[] body) {
			super(a, t, body);
		}
    	
    }
    
    
    private static MessageWithMetadata writeBuffer[] = null;
    private static MessageWithMetadata writeBuffer2[] = null;
    private static int writeBufferPtr = 0;
    
    private static void reserveWriteBuffer(int n)
    {
    	if (writeBuffer == null || writeBuffer.length < n) {
    		int oldSize;
    		MessageWithMetadata newBuffer[] = new MessageWithMetadata[nextSize(n)];
    		if (writeBuffer == null) {
    			oldSize = 0;
    		} else {
    			oldSize = writeBuffer.length;
    			System.arraycopy(writeBuffer, 0, newBuffer, 0, oldSize);
    		}
    		for (int i = oldSize; i < newBuffer.length; i++) {
    			newBuffer[i] = new MessageWithMetadata(0, 0, null);
    		}
    		writeBuffer = newBuffer;
    		writeBuffer2 = new MessageWithMetadata[newBuffer.length];
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
    
    
//    private static int writeBuffer2_QSortPartition(int l, int r) {
//		MessageWithMetadata pivot = writeBuffer2[r - 1];
//		  int k = l;
//		  for (int i = l; i < r - 1; i++) {
//		    if (writeBuffer2[i].getT() < pivot.getT()) {
//		    	MessageWithMetadata t = writeBuffer2[k];
//		      writeBuffer2[k] = writeBuffer2[i];
//		      writeBuffer2[i] = t;
//		      k++;
//		    }
//		  }
//		  MessageWithMetadata t = writeBuffer2[k];
//		  writeBuffer2[k] = pivot;
//		  writeBuffer2[r - 1] = t;
//		  return k;
//    }
//    private static void writeBuffer2_QSort(int l, int r) {
//	  if (l < r) {
//	    int m = writeBuffer2_QSortPartition(l, r);
//	    writeBuffer2_QSort(l, m);
//	    writeBuffer2_QSort(m + 1, r);
//	  }
//	}
	
    
    
    private static ByteBuffer pointWriteBuffer = null;
    private static void reservePointBuffer(int nBytes)
    {
    	if (pointWriteBuffer == null || pointWriteBuffer.capacity() < nBytes) {
    		pointWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		pointWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    
    private static ByteBuffer bodyWriteBuffer = null;
    private static void reserveBodyBuffer(int nBytes)
    {
    	if (bodyWriteBuffer == null || bodyWriteBuffer.capacity() < nBytes) {
    		bodyWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
    		bodyWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    	}
    }
    
    private static void writeBodyData() throws IOException
    {		
		int nThread = nPutThread;
		
		// 写线程t块偏移（供getMessage用）
		reserveBodyBuffer(nThread * 20);
		bodyWriteBuffer.position(0);
		for (int i = 0; i < nThread; i++) {
			bodyWriteBuffer.putInt(zpRecordOffset[i]);
			bodyWriteBuffer.putLong(zpByteOffset[i]);
			bodyWriteBuffer.putLong(zpLastT[i]);
		}
		tAxisBodyStream.write(bodyWriteBuffer.array(), 0, nThread * 20);
    }

    private static void flushWriteBuffer(long exclusiveT) throws IOException
    {
//		System.out.println(String.format("flush=%d size=%d", tSliceCount - 1, writeBuffer.size()));
		int nWrite;
		for (nWrite = 0; nWrite < writeBufferPtr; nWrite++) {
			MessageWithMetadata curMessage = writeBuffer[nWrite];
			if (curMessage.getT() == exclusiveT) {
				break;
			}
		}

		int tSliceId = tSliceCount - 1;
		
		tSliceRecordCount[tSliceId] = nWrite;


		for (int i = 0; i < nWrite; i++) {
			MessageWithMetadata msg = writeBuffer[i];
			int threadId = msg.threadId;
			zpRecordOffset[threadId] = msg.recordOffset;
			zpByteOffset[threadId] = msg.byteOffset;
			zpLastT[threadId] = msg.getT();
		}
		writeBodyData();

		// 写t轴压缩索引（为了保证t的压缩效率，t块内部应按照t排序
		reserveBodyBuffer(nWrite * 18);
		ByteBuffer compressedPointBuffer = bodyWriteBuffer; // 重用bodyWriteBuffer的空间，懒得再分开了
		compressedPointBuffer.position(0);
		
		long lastT = tSlicePivot[tSliceId];
		for (int i = 0; i < nWrite; i++) {
			MessageWithMetadata msg = writeBuffer[i];
			long a = msg.getA();
			long t = msg.getT();
			
			long deltaT = t - lastT;
			ValueCompressor.putToBuffer(compressedPointBuffer, deltaT);
			ValueCompressor.putToBuffer(compressedPointBuffer, a);
			lastT = t;
		}
		
		
		// 计算每小块内记录数量，并写每小格的数据，此时t块内部必须按a排序
		reservePointBuffer(nWrite * 16);
		pointWriteBuffer.position(0);
		System.arraycopy(writeBuffer, 0, writeBuffer2, 0, nWrite); // 记录按t排序的顺序到writeBuffer2
		Arrays.sort(writeBuffer, 0, nWrite, aComparator);
		int aSliceId = 0;
		
		int aSlice2Id = 0;
		int aSlice3Id = 0; 
		for (int i = 0; i < N_ASLICE2; i++) {
			aAxisCompressedPoint2BaseT.set(tSliceId + 1, i, aAxisCompressedPoint2BaseT.get(tSliceId, i));
		}
		for (int i = 0; i < N_ASLICE3; i++) {
			aAxisCompressedPoint3BaseT.set(tSliceId + 1, i, aAxisCompressedPoint3BaseT.get(tSliceId, i));
		}
		
		for (int i = 0; i < nWrite; i++) {
			MessageWithMetadata msg = writeBuffer[i];
			long a = msg.getA();
			long t = msg.getT();
			
			while (aSliceId < N_ASLICE && a >= aSlicePivot[aSliceId + 1]) aSliceId++;
			
			blockOffsetTableAxisT.inc(tSliceId, aSliceId);
			blockOffsetTableAxisA.inc(tSliceId, aSliceId);
			
			pointWriteBuffer.putLong(t).putLong(a);
			
			while (aSlice2Id < N_ASLICE2 && a >= aSlice2Pivot[aSlice2Id + 1]) aSlice2Id++;
			msg.aSliceId2 = aSlice2Id;
			
			while (aSlice3Id < N_ASLICE3 && a >= aSlice3Pivot[aSlice3Id + 1]) aSlice3Id++;
			msg.aSliceId3 = aSlice3Id;
		}
		
		
		ByteBuffer aAxisWriteBuffer = ByteBuffer.allocate(18).order(ByteOrder.LITTLE_ENDIAN);
		
		// 写a轴压缩索引
		for (int i = 0; i < nWrite; i++) {
			MessageWithMetadata msg = writeBuffer2[i];
			long a = msg.getA();
			long t = msg.getT();
			
			aSlice2Id = msg.aSliceId2;
			long deltaT2 = t - aAxisCompressedPoint2BaseT.get(tSliceId + 1, aSlice2Id);
			
			aAxisWriteBuffer.position(0);
			ValueCompressor.putToBuffer(aAxisWriteBuffer, deltaT2);
			ValueCompressor.putToBuffer(aAxisWriteBuffer, a);
			aAxisCompressedPoint2Data[aSlice2Id].write(aAxisWriteBuffer.array(), 0, aAxisWriteBuffer.position());
			aAxisCompressedPoint2ByteOffset.add(tSliceId, aSlice2Id, aAxisWriteBuffer.position());
			aAxisCompressedPoint2OutputBytes += aAxisWriteBuffer.position();
			aAxisCompressedPoint2BaseT.set(tSliceId + 1, aSlice2Id, t);
		}
		for (int i = 0; i < nWrite; i++) {
			MessageWithMetadata msg = writeBuffer2[i];
			long a = msg.getA();
			long t = msg.getT();
			
			aSlice3Id = msg.aSliceId3;
			long deltaT3 = t - aAxisCompressedPoint3BaseT.get(tSliceId + 1, aSlice3Id);
			
			aAxisWriteBuffer.position(0);
			ValueCompressor.putToBuffer(aAxisWriteBuffer, deltaT3);
			ValueCompressor.putToBuffer(aAxisWriteBuffer, a);
			aAxisCompressedPoint3Data[aSlice3Id].write(aAxisWriteBuffer.array(), 0, aAxisWriteBuffer.position());
			aAxisCompressedPoint3ByteOffset.add(tSliceId, aSlice3Id, aAxisWriteBuffer.position());
			aAxisCompressedPoint3OutputBytes += aAxisWriteBuffer.position();
			aAxisCompressedPoint3BaseT.set(tSliceId + 1, aSlice3Id, t);
		}
		
		shiftWriteBuffer(nWrite);
		
		tAxisPointStream.write(pointWriteBuffer.array(), 0, nWrite * 16);

		tAxisCompressedPointStream.write(compressedPointBuffer.array(), 0, compressedPointBuffer.position());
		tSliceCompressedPointByteOffset[tSliceId + 1] = tSliceCompressedPointByteOffset[tSliceId] + compressedPointBuffer.position();
    }

    private static BufferedOutputStream tAxisPointStream;
    private static BufferedOutputStream tAxisBodyStream;
    private static BufferedOutputStream tAxisCompressedPointStream;
    private static void beginInsertMessage() throws IOException
    {
    	reserveDiskSpace(tAxisPointFile, (long)globalTotalRecords * 16);
    	reserveDiskSpace(tAxisCompressedPointFile, (long)globalTotalRecords * 10);
    	tAxisPointStream = new BufferedOutputStream(new FileOutputStream(tAxisPointFile));
    	tAxisBodyStream = new BufferedOutputStream(new FileOutputStream(tAxisBodyFile));
    	tAxisCompressedPointStream = new BufferedOutputStream(new FileOutputStream(tAxisCompressedPointFile));
    	
    	writeBodyData();
    }
    private static void insertMessage(long curT, long curA, int threadId, int nextRecordId, long nextRecordByteOffset) throws IOException
    {
    	if (insCount % TSLICE_INTERVAL == 0) {
			if (insCount > 0) {
				flushWriteBuffer(curT);
			}
			tSlicePivot[tSliceCount++] = curT;

		}
		
		reserveWriteBuffer(writeBufferPtr + 1);
		MessageWithMetadata message = writeBuffer[writeBufferPtr++];
		message.setT(curT);
		message.setA(curA);

		message.threadId = threadId;
		message.recordOffset = nextRecordId;
		message.byteOffset = nextRecordByteOffset;
		
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
    	
    	System.out.println("tSliceCount=" + tSliceCount);
    	System.out.println(String.format("tSliceCompressedPointBytes=%d  (%f b/rec)", tSliceCompressedPointByteOffset[tSliceCount], (double)tSliceCompressedPointByteOffset[tSliceCount]/insCount));
    	System.out.println(String.format("aAxisCompressedPoint2OutputBytes=%d  (%f b/rec)", aAxisCompressedPoint2OutputBytes, (double)aAxisCompressedPoint2OutputBytes / insCount));
    	System.out.println(String.format("aAxisCompressedPoint3OutputBytes=%d  (%f b/rec)", aAxisCompressedPoint3OutputBytes, (double)aAxisCompressedPoint3OutputBytes / insCount));
    	
    	tAxisPointStream.close();
    	tAxisPointStream = null;
    	tAxisBodyStream.close();
    	tAxisBodyStream = null;
    	tAxisCompressedPointStream.close();
    	tAxisCompressedPointStream = null;
    	
    	for (int i = 0; i < N_ASLICE2; i++) {
    		aAxisCompressedPoint2Data[i].close();
    		aAxisCompressedPoint2Data[i] = null;
    	}
    	for (int i = 0; i < N_ASLICE3; i++) {
    		aAxisCompressedPoint3Data[i].close();
    		aAxisCompressedPoint3Data[i] = null;
    	}
    }
    



    private static void externalMergeSort() throws IOException
    {
    	System.out.println("[" + new Date().toString() + "]: merge-sort begin!");

		int nThread = nPutThread;
		
		ByteBuffer queueData[] = new ByteBuffer[nThread];
		for (int i = 0; i < nThread; i++) {
			queueData[i] = ByteBuffer.allocate(4096);
			queueData[i].order(ByteOrder.LITTLE_ENDIAN);
		}
		
		int readCount[] = new int[nThread]; 
		int recordCount[] = new int[nThread];
		int bufferCap[] = new int[nThread];
		long readBytes[] = new long[nThread];
		long queueHead[] = new long[nThread];
		
		for (int i = 0; i < nThread; i++) {
			recordCount[i] = putTLD[i].outputCount;
			readCount[i] = 0;
			
			if (recordCount[i] > 0) {
				putTLD[i].pointInputStream = new FileInputStream(putTLD[i].pointFileName);
				readBytes[i] = bufferCap[i] = putTLD[i].pointInputStream.read(queueData[i].array(), 0, queueData[i].capacity());
				queueHead[i] = ValueCompressor.getFromBuffer(queueData[i]);
			} else {
				queueHead[i] = Long.MAX_VALUE;
			}
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
			
			long aValue = ValueCompressor.getFromBuffer(queueData[minPos]); 
			insertMessage(minValue, aValue, minPos, readCount[minPos] + 1, readBytes[minPos] - bufferCap[minPos] + queueData[minPos].position());
			
			if (++readCount[minPos] >= recordCount[minPos]) {
				queueHead[minPos] = Long.MAX_VALUE;
			} else {
				ByteBuffer buffer = queueData[minPos];
				if (buffer.remaining() < 64) {
					int nCopy = bufferCap[minPos] - buffer.position();
					System.arraycopy(buffer.array(), buffer.position(), buffer.array(), 0, nCopy);
					buffer.position(0);
					int nReadBytes = (int)Math.min(buffer.capacity() - nCopy, putTLD[minPos].outputBytes - readBytes[minPos]);
					if (nReadBytes > 0) {
						putTLD[minPos].pointInputStream.read(buffer.array(), nCopy, nReadBytes);
						readBytes[minPos] += nReadBytes;
					}
					bufferCap[minPos] = nCopy + nReadBytes;
				}
				
				queueHead[minPos] += ValueCompressor.getFromBuffer(buffer);
			}
		}
		
		finishInsertMessage();
		
		for (int i = 0; i < nThread; i++) {
			putTLD[i].pointInputStream.close();
			putTLD[i].pointInputStream = null;
		}
		System.out.println("[" + new Date().toString() + "]: merge-sort completed!");
    }
    
    
    
    
    
    
    
    
    
    private static void calcPivotAxisA()
    {
    	// 计算A的范围
		int nThread = nPutThread;
    	for (int i = 0; i < nThread; i++) {
    		globalMaxA = Math.max(globalMaxA, putTLD[i].maxA);
    		globalMinA = Math.min(globalMinA, putTLD[i].minA);
    	}
		System.out.println(String.format("globalMinA=%d", globalMinA));
		System.out.println(String.format("globalMaxA=%d", globalMaxA));
		
		// 计算样本的n分位数，作为a轴上的分割点
		Collections.sort(aSamples);
    	for (int i = 0; i < N_ASLICE; i++) {
    		aSlicePivot[i] = aSamples.get(aSamples.size() / N_ASLICE * i).longValue();
    	}
    	aSlicePivot[0] = globalMinA;
    	aSlicePivot[N_ASLICE] = Long.MAX_VALUE;
    	for (int i = 0; i <= N_ASLICE; i++) {
    		System.out.println(String.format("aSlicePivot[%d]=%d", i, aSlicePivot[i]));
    	}
    	
    	// 计算2号a索引的分割点
    	for (int i = 0; i < N_ASLICE2; i++) {
    		aSlice2Pivot[i] = aSamples.get(aSamples.size() / N_ASLICE2 * i).longValue();
    	}
    	aSlice2Pivot[0] = globalMinA;
    	aSlice2Pivot[N_ASLICE2] = Long.MAX_VALUE;
    	for (int i = 0; i <= N_ASLICE2; i++) {
    		System.out.println(String.format("aSlice2Pivot[%d]=%d", i, aSlice2Pivot[i]));
    	}
    	
    	// 计算3号a索引的分割点
    	for (int i = 0; i < N_ASLICE3 - 1; i++) {
    		aSlice3Pivot[i + 1] = aSamples.get(aSamples.size() / (N_ASLICE3 - 1) / 2 + aSamples.size() / (N_ASLICE3 - 1) * i).longValue();
    	}
    	aSlice3Pivot[0] = globalMinA;
    	aSlice3Pivot[N_ASLICE3] = Long.MAX_VALUE;
    	for (int i = 0; i <= N_ASLICE3; i++) {
    		System.out.println(String.format("aSlice3Pivot[%d]=%d", i, aSlice3Pivot[i]));
    	}
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    private static final int SAMPLE_P = MAXMSG / 10000;
    private static final ArrayList<Long> aSamples = new ArrayList<Long>();
    

    
    private static class PutThreadLocalData {
    	ByteBuffer pointBuffer;
    	
    	BufferedOutputStream bufferedPointOutputStream;
    	BufferedOutputStream bufferedBodyOutputStream;
    	FileInputStream pointInputStream;
    	
    	long lastT = 0;
    	
    	int outputCount = 0;
    	long outputBytes = 0;
    	
    	
    	long maxA = Long.MIN_VALUE;
    	long minA = Long.MAX_VALUE;
    	
    	int threadId;
    	String pointFileName;
    	String bodyFileName;
    	
    	FileChannel zpChannel;
    	FileChannel bodyChannel;
    	
    }
    
    private static final PutThreadLocalData putTLD[] = new PutThreadLocalData[MAXTHREAD];
    private static final AtomicInteger putThreadCount = new AtomicInteger();
    private static int nPutThread;
    private static final ThreadLocal<PutThreadLocalData> putBuffer = new ThreadLocal<PutThreadLocalData>() {
        @Override protected PutThreadLocalData initialValue() {
        	
        	PutThreadLocalData pd = new PutThreadLocalData();
        	pd.threadId = putThreadCount.getAndIncrement();
        	putTLD[pd.threadId] = pd;
        	
        	pd.pointBuffer = ByteBuffer.allocate(18).order(ByteOrder.LITTLE_ENDIAN);
        	
        	pd.pointFileName = String.format("thread%04d.zp.data", pd.threadId);
        	pd.bodyFileName = String.format("thread%04d.body.data", pd.threadId);
        	
        	try {
        		pd.bufferedPointOutputStream = new BufferedOutputStream(new FileOutputStream(pd.pointFileName));
        		pd.bufferedBodyOutputStream = new BufferedOutputStream(new FileOutputStream(pd.bodyFileName));
        		
        		pd.zpChannel = FileChannel.open(Paths.get(pd.pointFileName));
        		pd.bodyChannel = FileChannel.open(Paths.get(pd.bodyFileName));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
        	
        	return pd;
        }
    };
    
    
    private static void flushPutBuffer() throws IOException
    {
    	System.out.println("[" + new Date() + "]: flushing remaining buffers ...");
    	globalTotalRecords = 0;
    	nPutThread = putThreadCount.get();
		int nThread = nPutThread;
		for (int i = 0; i < nThread; i++) {
			PutThreadLocalData pd = putTLD[i];
			
			pd.bufferedPointOutputStream.close();
			pd.bufferedPointOutputStream = null;
			
			pd.bufferedBodyOutputStream.close();
			pd.bufferedBodyOutputStream = null;
			
			System.out.println(String.format("thread %d: %d (%d bytes, %f b/rec)", i, pd.outputCount, pd.outputBytes, (double)pd.outputBytes / pd.outputCount));
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
    		
    		ByteBuffer pointBuffer = pd.pointBuffer;
    		pointBuffer.position(0);
    		
    		ValueCompressor.putToBuffer(pointBuffer, curT - pd.lastT);
    		pd.lastT = curT;
    		ValueCompressor.putToBuffer(pointBuffer, curA);
    		
    		pd.bufferedPointOutputStream.write(pointBuffer.array(), 0, pointBuffer.position());
    		pd.outputBytes += pointBuffer.position();
    		
    		pd.bufferedBodyOutputStream.write(message.getBody());
    		
    		pd.outputCount++;
    		pd.maxA = Math.max(pd.maxA, curA);
    		pd.minA = Math.min(pd.minA, curA);
    		
    		// 从数据中抽样一些数据，用于计算A的分割点
    		if (ThreadLocalRandom.current().nextInt(SAMPLE_P) == 0) {
    			synchronized (aSamples) {
    				aSamples.add(Long.valueOf(curA));
    			}
    		}
    		
		} catch (IOException e) {
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

//    				firstFlag = true;
    				
    				state = 2;
    			}
    		}
    	}
    	
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	int nThread = nPutThread;
    	
		try {
			ByteBuffer tSliceLowOffsetBuffer = ByteBuffer.allocate(nThread * 20);
			tSliceLowOffsetBuffer.order(ByteOrder.LITTLE_ENDIAN);
			tAxisBodyChannel.read(tSliceLowOffsetBuffer, (long)tSliceLow * nThread * 20);
			
			ByteBuffer tSliceHighOffsetBuffer = ByteBuffer.allocate(nThread * 20);
			tSliceHighOffsetBuffer.order(ByteOrder.LITTLE_ENDIAN);
			tAxisBodyChannel.read(tSliceHighOffsetBuffer, (long)(tSliceHigh + 1) * nThread * 20); // exclusive
			
			tSliceLowOffsetBuffer.position(0);
			tSliceHighOffsetBuffer.position(0);
			for (int threadId = 0; threadId < nThread; threadId++) {
				
				int recordOffset = tSliceLowOffsetBuffer.getInt();
				int nRecord = tSliceHighOffsetBuffer.getInt() - recordOffset;
				long byteOffset = tSliceLowOffsetBuffer.getLong();
				int nBytes = (int)(tSliceHighOffsetBuffer.getLong() - byteOffset);
				long t = tSliceLowOffsetBuffer.getLong();
				tSliceHighOffsetBuffer.getLong();
				
				ByteBuffer zpBuffer = ByteBuffer.allocate(nBytes);
				zpBuffer.order(ByteOrder.LITTLE_ENDIAN);
	    		ByteBuffer bodyBuffer = ByteBuffer.allocate(nRecord * 34);
	    		bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
	    		
	    		putTLD[threadId].bodyChannel.read(bodyBuffer, (long)recordOffset * 34);
	    		putTLD[threadId].zpChannel.read(zpBuffer, byteOffset);
				
	    		zpBuffer.position(0);

	    		for (int recordId = 0; recordId < nRecord; recordId++) {
					
	    			try {
	    				t += ValueCompressor.getFromBuffer(zpBuffer);
	    				long a = ValueCompressor.getFromBuffer(zpBuffer);
	    				
						if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
							byte body[] = new byte[34];
							bodyBuffer.position(recordId * 34);
							bodyBuffer.get(body);
							result.add(new Message(a, t, body));
//							System.out.println(String.format("tLow=%d tHigh=%d nBytes=%d nRecord=%d tid=%d rid=%d t=%d a=%d", tSliceLow, tSliceHigh, nBytes, nRecord, threadId, recordId, t, a));
							assert ByteBuffer.wrap(body).getLong() == t;
						}
						
	    			} catch (BufferUnderflowException e) {
	    				e.printStackTrace();
	    				System.out.println(String.format("ERROR! (%d %d %d %d) tSliceLow=%d tSliceHigh=%d tid=%d rid=%d nr=%d nb=%d", tMin, tMax, aMin, aMax, tSliceLow, tSliceHigh, threadId, recordId, nRecord, nBytes));
	    				System.exit(-1);
	    			}

				}
				
				assert !zpBuffer.hasRemaining();
	
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		Collections.sort(result, tComparator); // FIXME: 用合并排序提高性能

//		System.out.println("[" + new Date() + "]: " + String.format("queryData: [%d %d] (%d %d %d %d) => %d", tMax-tMin, aMax-aMin, tMin, tMax, aMin, aMax, result.size()));

		
		
//		if (firstFlag) {
//			// 预热JVM
//			System.out.println("[" + new Date() + "]: prepare JVM for stage3 started");
//			for (forcePlanId = 0; forcePlanId < MAXPLAN; forcePlanId++) {
//				for (int i = 0; i < 10000; i++) {
//					getAvgValue(aMin, aMax, tMin, tMax);
//				}
//			}
//			forcePlanId = -1;
//			resetQueryStatistics();
//			System.out.println("[" + new Date() + "]: prepare JVM for stage3 finished");
//		}
		
		
    	return result;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    private static int forcePlanId = -1; // 预热/调试时候用
    
    
    private static final AverageResult queryTLD[] = new AverageResult[MAXTHREAD];
    private static final AtomicInteger queryThreadCount = new AtomicInteger();
    private static final ThreadLocal<AverageResult> averageResult = new ThreadLocal<AverageResult>() {
        @Override protected AverageResult initialValue() {
        	return queryTLD[queryThreadCount.getAndIncrement()];
        }
    };
    
    static {
    	for (int i = 0; i < MAXTHREAD; i++) {
    		queryTLD[i] = new AverageResult();
    	}
    }
    
    
    private static final int MAXPLAN = 5;
    private static final double IOSIZE_FACTOR = 27400; // SSD速度为 200MB/s 10000IOPS  这样算每个IO大约20KB
    
    private static class AverageResult {
    	long sum;
    	int cnt;
    	
    	//////////////
    	
    	long tMin;
    	long tMax;
    	long aMin;
    	long aMax;
    	
    	int tSliceLow;
    	int tSliceHigh;
    	int aSliceLow;
    	int aSliceHigh;
    	int aSlice2Low;
    	int aSlice2High;
    	int aSlice3Low;
    	int aSlice3High;
    	
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
    	
    	//////////////
    	
    	void reset()
    	{
    		sum = 0;
    		cnt = 0;
    		
    		tAxisIOCount = 0;
    		tAxisIOBytes = 0;
    		aAxisIOCount = 0;
    		aAxisIOBytes = 0;
    	}
    }
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////
    // 算法0：查询矩形的四个边
    
    private static void queryAverageSliceT(AverageResult result, boolean doRealQuery, int tSliceId, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = blockOffsetTableAxisT.get(tSliceId, aSliceLow);
		int nRecord = blockOffsetTableAxisT.get(tSliceId, aSliceHigh + 1) - baseOffset;
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++;
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

    	int baseOffsetLow = blockOffsetTableAxisA.get(tSliceLow, aSliceLow);
		int nRecordLow = blockOffsetTableAxisA.get(tSliceHigh + 1, aSliceLow) - baseOffsetLow;

		
		int baseOffsetHigh = blockOffsetTableAxisA.get(tSliceLow, aSliceHigh);
		int nRecordHigh = blockOffsetTableAxisA.get(tSliceHigh + 1, aSliceHigh) - baseOffsetHigh;

		
		if (aSliceLow != aSliceHigh) {
			result.addIOCost((long)nRecordLow * 8);
			result.addIOCost((long)nRecordHigh * 8);
		} else {
			result.addIOCost((long)nRecordLow * 8);
		}
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
		if (aSliceLow != aSliceHigh) {
			aAxisIndexChannel.read(highBuffer, (long)baseOffsetHigh * 8);
		} else {
			highBuffer.put(lowBuffer);
		}
		highBuffer.position(0);
		LongBuffer highBufferL = highBuffer.asLongBuffer();
		
		int lowOffset = 0;
		int highOffset = 0;
		for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
			
			int lowCount = blockOffsetTableAxisA.get(tSliceId + 1, aSliceLow) - blockOffsetTableAxisA.get(tSliceId, aSliceLow) - 1;
			int highCount = blockOffsetTableAxisA.get(tSliceId + 1, aSliceHigh) - blockOffsetTableAxisA.get(tSliceId, aSliceHigh) - 1;
			
			long lastPrefixSum = lowBufferL.get(lowOffset + lowCount);
			long lowSum = lastPrefixSum;
			int lowPtr = -1;
			for (int i = lowOffset; i < lowOffset + lowCount; i++) {
				long prefixSum = lowBufferL.get(i);
				long a = prefixSum - lastPrefixSum;
				lastPrefixSum = prefixSum;
//				System.out.println(String.format("low a=%d", a));
				if (a < aMin) {
					lowSum = prefixSum;
					lowPtr = i - lowOffset;
				} else {
					break;
				}
			}
			lowPtr++;
			
			lastPrefixSum = highBufferL.get(highOffset + highCount);
			long highSum = lastPrefixSum;
			int highPtr = -1;
			for (int i = highOffset; i < highOffset + highCount; i++) {
				long prefixSum = highBufferL.get(i);
				long a = prefixSum - lastPrefixSum;
				lastPrefixSum = prefixSum;
				if (a <= aMax) {
					highSum = highBufferL.get(i);
					highPtr = i - highOffset;
				}
			}
			
			
			int globalLowPtr = blockOffsetTableAxisT.get(tSliceId, aSliceLow) + lowPtr;
			int globalHighPtr = blockOffsetTableAxisT.get(tSliceId, aSliceHigh) + highPtr;
			
			long sum = 0;
			int cnt = 0;
			if (globalHighPtr >= globalLowPtr) {
				sum = highSum - lowSum;
				cnt = globalHighPtr - globalLowPtr + 1;
			}
			
			result.sum += sum;
			result.cnt += cnt;
		
//			AverageResult referenceResult = new AverageResult();
//			queryAverageSliceT(referenceResult, doRealQuery, tSliceId, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
//			System.out.println(String.format("tSliceId=%d ; aSliceLow=%d aSliceHigh=%d ; lowPtr=%d  highPtr=%d", tSliceId, aSliceLow, aSliceHigh, lowPtr, highPtr));
//			System.out.println(String.format("cnt=%d sum=%d", cnt, sum));
//			System.out.println(String.format("ref: cnt=%d sum=%d", referenceResult.cnt, referenceResult.sum));
//			assert cnt == referenceResult.cnt;
//			assert sum == referenceResult.sum;

			
			lowOffset += lowCount + 1;
			highOffset += highCount + 1;
		}
		
		assert lowOffset == nRecordLow;
		assert highOffset == nRecordHigh;
    }
    
    private static void queryAlgorithm0(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSliceLow = result.aSliceLow;
    	int aSliceHigh = result.aSliceHigh;
    	
    	if (tSliceLow == tSliceHigh) {
    		// 在同一个a块内，只能暴力
    		queryAverageSliceT(result, doRealQuery, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		
    	} else {
    		
    		queryAverageSliceT(result, doRealQuery, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		queryAverageSliceT(result, doRealQuery, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		tSliceLow++;
    		tSliceHigh--;
    		if (tSliceLow <= tSliceHigh) {
    			queryAverageSliceA(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		}
    	}
    }
    
    
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法1：对t轴上的分块进行暴力查找
    
    private static void queryAlgorithm1(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSliceLow = result.aSliceLow;
    	int aSliceHigh = result.aSliceHigh;
    	
		int baseOffset = blockOffsetTableAxisT.get(tSliceLow, aSliceLow);
		int nRecord = blockOffsetTableAxisT.get(tSliceHigh, aSliceHigh + 1) - baseOffset;
		
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++; // FIXME: 分开统计？
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
	// 算法2：对t轴上的压缩分块进行暴力查找
    
    private static void queryAlgorithm2(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	
		long baseOffset = tSliceCompressedPointByteOffset[tSliceLow];
		long nBytes = tSliceCompressedPointByteOffset[tSliceHigh + 1] - baseOffset;
		result.addIOCost(nBytes);
		if (!doRealQuery) return;
		result.tAxisIOCount++; // FIXME: 分开统计？
		result.tAxisIOBytes += nBytes;
		
		
		ByteBuffer pointBuffer = ByteBuffer.allocateDirect((int)nBytes);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisCompressedPointChannel.read(pointBuffer, baseOffset);
		pointBuffer.position(0);
		
		for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
			int nRecord = tSliceRecordCount[tSliceId];
			long t = tSlicePivot[tSliceId];
			for (int i = 0; i < nRecord; i++) {
				t += ValueCompressor.getFromBuffer(pointBuffer);
				long a = ValueCompressor.getFromBuffer(pointBuffer);
//				System.out.println("t=" + t + " a=" + a);
		
				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
					result.sum += a;
					result.cnt++;
				}
			}
		}
		assert pointBuffer.position() == pointBuffer.capacity();
    }
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法3：对a轴上的压缩分块进行暴力查找
    
    private static void queryAZP2(AverageResult result, boolean doRealQuery, int aSlice2Id, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	long nBytes = aAxisCompressedPoint2ByteOffset.get(tSliceHigh + 1, aSlice2Id) - aAxisCompressedPoint2ByteOffset.get(tSliceLow, aSlice2Id);
		
    	result.addIOCost(nBytes);
		if (!doRealQuery) return;
		result.aAxisIOCount++; // FIXME: 分开统计？
		result.aAxisIOBytes += nBytes;
		
    	ByteBuffer buffer = ByteBuffer.allocateDirect((int)nBytes).order(ByteOrder.LITTLE_ENDIAN);
    	aAxisCompressedPoint2Channel[aSlice2Id].read(buffer, aAxisCompressedPoint2ByteOffset.get(tSliceLow, aSlice2Id));
    	buffer.position(0);
    	
    	
    	long t = aAxisCompressedPoint2BaseT.get(tSliceLow, aSlice2Id);
		while (buffer.hasRemaining()) {
			t += ValueCompressor.getFromBuffer(buffer);
			long a = ValueCompressor.getFromBuffer(buffer);
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    private static void queryAlgorithm3(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSlice2Low = result.aSlice2Low;
    	int aSlice2High = result.aSlice2High;
    	
    	for (int aSlice2Id = aSlice2Low; aSlice2Id <= aSlice2High; aSlice2Id++) {
    		queryAZP2(result, doRealQuery, aSlice2Id, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    	}
    }
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法4：对a轴上的3号压缩分块进行暴力查找
    
    private static void queryAZP3(AverageResult result, boolean doRealQuery, int aSlice3Id, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	long nBytes = aAxisCompressedPoint3ByteOffset.get(tSliceHigh + 1, aSlice3Id) - aAxisCompressedPoint3ByteOffset.get(tSliceLow, aSlice3Id);
		
    	result.addIOCost(nBytes);
		if (!doRealQuery) return;
		result.aAxisIOCount++; // FIXME: 分开统计？
		result.aAxisIOBytes += nBytes;
		
    	ByteBuffer buffer = ByteBuffer.allocateDirect((int)nBytes).order(ByteOrder.LITTLE_ENDIAN);
    	aAxisCompressedPoint3Channel[aSlice3Id].read(buffer, aAxisCompressedPoint3ByteOffset.get(tSliceLow, aSlice3Id));
    	buffer.position(0);
    	
    	long t = aAxisCompressedPoint3BaseT.get(tSliceLow, aSlice3Id);
		while (buffer.hasRemaining()) {
			t += ValueCompressor.getFromBuffer(buffer);
			long a = ValueCompressor.getFromBuffer(buffer);
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    private static void queryAlgorithm4(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSlice3Low = result.aSlice3Low;
    	int aSlice3High = result.aSlice3High;
    	
    	for (int aSlice3Id = aSlice3Low; aSlice3Id <= aSlice3High; aSlice3Id++) {
    		queryAZP3(result, doRealQuery, aSlice3Id, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    	}
    }
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////

    ///// 查询执行器：给定算法Id号，执行对应的查询
    private static void queryExecutor(AverageResult result, int planId, boolean doRealQuery) throws IOException
    {
    	result.curPlan = planId;
    	result.ioCost[planId] = 0;
    	
    	switch (planId) {
    	case 0: queryAlgorithm0(result, doRealQuery); break;
    	case 1: queryAlgorithm1(result, doRealQuery); break;
    	case 2: queryAlgorithm2(result, doRealQuery); break;
    	case 3: queryAlgorithm3(result, doRealQuery); break;
    	case 4: queryAlgorithm4(result, doRealQuery); break;
    	default: assert false;
    	}
    }
    
    
    ////// 查询计划器：预估不同查询算法IO代价，选择IO代价最小的算法Id号返回
    private static int queryPlanner(AverageResult result, boolean doRealQuery) throws IOException
    {
    	if (forcePlanId >= 0) {
    		return forcePlanId;
    	}
    	
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		queryExecutor(result, planId, false);
    	}
    	
    	double minIOCost = 1e100;
    	int optimalPlanId = -1;
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		if (result.ioCost[planId] < minIOCost) {
    			minIOCost = result.ioCost[planId];
    			optimalPlanId = planId;
    		}
    	}
    	
//    	optimalPlanId = 2;
    	return optimalPlanId;
    }
    

    
    ////////////////////////////////////////////////////////////////////////////////////////

    
    private static final AtomicInteger totalAvgQuery = new AtomicInteger();
    private static final AtomicLong totalAvgRecords = new AtomicLong();
	private static final AtomicLong tAxisIOCountTotal = new AtomicLong();
	private static final AtomicLong tAxisIOBytesTotal = new AtomicLong();
	private static final AtomicLong aAxisIOCountTotal = new AtomicLong();
	private static final AtomicLong aAxisIOBytesTotal = new AtomicLong();
	private static final DoubleAdder totalIOCost = new DoubleAdder();
	private static final AtomicIntegerArray planCount = new AtomicIntegerArray(MAXPLAN);
	
	private static void resetQueryStatistics()
	{
		queryThreadCount.set(0);
		
	    totalAvgQuery.set(0);
	    totalAvgRecords.set(0);
		tAxisIOCountTotal.set(0);
		tAxisIOBytesTotal.set(0);
		aAxisIOCountTotal.set(0);
		aAxisIOBytesTotal.set(0);
		totalIOCost.reset();
		for (int i = 0; i < MAXPLAN; i++) {
			planCount.set(i, 0);
		}
	}
	
    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {

    	AverageResult result = averageResult.get();
    	result.reset();
    	
    	result.tMin = tMin;
    	result.tMax = tMax;
    	result.aMin = aMin;
    	result.aMax = aMax;
    	
    	result.tSliceLow = findSliceT(tMin);
    	result.tSliceHigh = findSliceT(tMax);
    	result.aSliceLow = findSliceA(aMin);
    	result.aSliceHigh = findSliceA(aMax);
    	result.aSlice2Low = findSliceA2(aMin);
    	result.aSlice2High = findSliceA2(aMax);
    	result.aSlice3Low = findSliceA3(aMin);
    	result.aSlice3High = findSliceA3(aMax);
    	
//    	System.out.println(String.format("block: t[%d %d] a[%d %d]", tSliceLow, tSliceHigh, aSliceLow, aSliceHigh));  
    	try {

    		// 不同查询算法的IO代价可能不同
    		// 这里模仿数据库的查询计划器，先预估每种算法的IO代价，挑选最小的那个去执行
    		int optimalPlanId = queryPlanner(result, false);
    		queryExecutor(result, optimalPlanId, true);
	    	
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
//    	System.out.println("[" + new Date() + "]: " + String.format("queryAverage: [t %d; a %d (%f)]; (%d %d %d %d) => cnt=%d; plan=%d [%f %f]; (t %d %d) (a %d %d)", tMax-tMin, aMax-aMin, (double)(aMax-aMin)/(globalMaxA - globalMinA), tMin, tMax, aMin, aMax, result.cnt, result.curPlan, result.ioCost[0], result.ioCost[1], result.tAxisIOCount, result.tAxisIOBytes, result.aAxisIOCount, result.aAxisIOBytes));
    	
    	
    	
    	totalAvgQuery.incrementAndGet();
    	totalAvgRecords.addAndGet(result.cnt);
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
    	
    	
    	System.out.println(String.format("totalAvgRecords=%d", totalAvgRecords.get()));
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
