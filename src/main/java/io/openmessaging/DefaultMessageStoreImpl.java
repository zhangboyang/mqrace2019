package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import sun.nio.ch.FileChannelImpl;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

	static long makeLong(int high, int low)
	{
		return ((long)high << 32) | ((long)low & 0xFFFFFFFFL);
	}
	
	static boolean rectOverlap(int aLeft, int aRight, int aBottom, int aTop, int bLeft, int bRight, int bBottom, int bTop)
	{
		return (aLeft <= bRight && aRight >= bLeft && aTop >= bBottom && aBottom <= bTop);
	}
	
	static boolean pointInRect(int lr, int bt, int rectLeft, int rectRight, int rectBottom, int rectTop)
	{
		return (rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop);
	}
	
	static boolean rectInRect(int aLeft, int aRight, int aBottom, int aTop, int bLeft, int bRight, int bBottom, int bTop)
	{
		return bLeft <= aLeft && aRight <= bRight && bBottom <= aBottom && aTop <= bTop;
	}
    
	static boolean pointInRectL(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop)
	{
		return (rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop);
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
    static class MessageCompressor {
    	// compress Message to long
    	// if compressible,   isValid(ret) == true
    	// if incompressible, isValid(ret) == false
    	
    	static final byte[] bodyTemplate = hexStringToByteArray("0000000000158BE00000000000160BE50D2125260B5E5B2B0C3741265C0C36070000");
    			
    	private static byte[] getBody(long t, long a)
    	{
    		ByteBuffer buffer = ByteBuffer.allocate(34);
    		buffer.put(bodyTemplate);
    		buffer.putLong(0, t);
    		buffer.putLong(8, a);
    		return buffer.array();
    	}
    	public static int doCompress(int tBase, Message message)
    	{
    		long t = message.getT();
    		long a = message.getA();
    		
    		long o = t - tBase;
    		long d = a - tBase + 10000;
    		if (t > 0 && (0 <= o && o < 256) && (0 < d && d < 65536) && Arrays.equals(getBody(t, a), message.getBody())) {
    			return ((int)d & 0xFFFF) | ((int)o << 16);
    		} else {
    			return 0;
    		}
    	}
    	public static boolean isValid(int m)
    	{
    		return (m & 0xFFFFFF) != 0;
    	}
    	public static int extractT(int tBase, int m)
    	{
    		return tBase + ((m >> 16) & 0xFF);
    	}
    	public static int extractA(int tBase, int m)
    	{
    		return tBase + (m & 0xFFFF) - 10000;
    	}
    	public static Message doDecompress(int tBase, int m)
    	{
    		/*ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(0, extractT(m));
    		return new Message(extractT(m), extractA(m), buffer.array());*/
    		int t = extractT(tBase, m);
    		int a = extractA(tBase, m);
    		return new Message(a, t, getBody(t, a));
    	}
    	public static String dumpMessage(Message message)
    	{
    		char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	    	StringBuilder s = new StringBuilder();
	    	s.append(String.format("%08X,", message.getT()));
	    	s.append(String.format("%08X,", message.getA()));
	    	byte[] bytes = message.getBody();
	    	for (int j = 0; j < bytes.length; j++) {
	            int v = bytes[j] & 0xFF;
	            s.append(HEX_ARRAY[v >>> 4]);
	            s.append(HEX_ARRAY[v & 0x0F]);
	        }
	    	return s.toString();
    	}
    };
    
    private static final Unsafe unsafe;
    
    private static final long MEMSZ = 6 * 1024 * 1048576L;
    private static final long memBase;
    
    static {
        Unsafe theUnsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            theUnsafe = null;
        }
        unsafe = theUnsafe;
        
        memBase = unsafe.allocateMemory(MEMSZ);
        unsafe.setMemory(memBase, MEMSZ, (byte)0);
    }
    
    
    
    private static final int I_SIZE = 8; // index-record size
    
    private static final int I_MINT = 0;
    private static final int I_MAXT = 1;
    private static final int I_MINA = 2;
    private static final int I_MAXA = 3;
    private static final int I_SUML = 4;
    private static final int I_SUMH = 5;
    private static final int I_CNT  = 6;
    private static final int I_TBASE = 7;
    
    private static final int H = 24; // max height of HEAP
    private static final int HEAP_ARRAY_SIZE = ((1 << (H + 1)) + 1);
    private static final int HEAP_LEAF_BASE = 1 << H;
    private static final long heapBase;
    
    static {
    	int arraySize = HEAP_ARRAY_SIZE * I_SIZE;
        heapBase = unsafe.allocateMemory(arraySize * 4);
        unsafe.setMemory(heapBase, arraySize * 4, (byte)0);
    }
    
    private static int indexHeap(int offset)
    {
    	return unsafe.getInt(heapBase + offset * 4L);
    }
    private static long indexHeapL(int offset)
    {
    	return unsafe.getLong(heapBase + offset * 4L);
    }
    private static void indexHeapSet(int offset, int val)
    {
    	unsafe.putInt(heapBase + offset * 4L, val);
    }
    
    private static final int L_NREC = 128; // n-record in one block

    private static volatile int state = 0;
    private static Object stateLock = new Object();
    
    private static boolean haveUncompressibleRecord = false;
    private static ArrayList<Message> uncompressibleRecords = new ArrayList<Message>();
    
    private static void updateLeafTBase(int leafBlockId, int tBase)
    {
    	indexHeapSet((HEAP_LEAF_BASE + leafBlockId) * I_SIZE + I_TBASE, tBase);
    }
    private static void updateLeafIndex(int leafBlockId)
    {
    	int base = (HEAP_LEAF_BASE + leafBlockId) * I_SIZE;
    	int tBase = indexHeap(base + I_TBASE);
    	
    	int minT = Integer.MAX_VALUE;
		int maxT = Integer.MIN_VALUE;
		int minA = Integer.MAX_VALUE;
		int maxA = Integer.MIN_VALUE;
		long sumA = 0;
		int cnt  = 0;
		
		long l = (long)leafBlockId * L_NREC;
		long r = l + L_NREC;
		for (long i = l; i < r; i++) {
			
			int m = unsafe.getInt(memBase + i * 3);
			
			if (MessageCompressor.isValid(m)) {
				int t = MessageCompressor.extractT(tBase, m);
				int a = MessageCompressor.extractA(tBase, m);
				
				minT = Math.min(minT, t);
				maxT = Math.max(maxT, t);
				minA = Math.min(minA, a);
				maxA = Math.max(maxA, a);
				
				sumA += a;
				cnt++;
			}
		}
		
		
		indexHeapSet(base + I_MINT, minT); 
		indexHeapSet(base + I_MAXT, maxT);
		indexHeapSet(base + I_MINA, minA);
		indexHeapSet(base + I_MAXA, maxA);
		indexHeapSet(base + I_SUML, (int)(sumA & 0xFFFFFFFF));
		indexHeapSet(base + I_SUMH, (int)(sumA >> 32));
		indexHeapSet(base + I_CNT , cnt);
    }
    
    
    
    private static int realRecordId = 0;
    private static long recordId = 0;
    private static int curTBase = 0;
    private static int unfullBlocks = 0;
    
    
    private void doPutMessage(Message message)
    {

    	/*if (realRecordId < 100000)
    	{
    		System.out.println(realRecordId + ", " + recordId + ", " + MessageCompressor.dumpMessage(message));
    	} else {
    		System.exit(-1);
    	}*/
    	
    	// 如果是一个新块，则更新 tBase
    	if (recordId % L_NREC == 0) {
    		curTBase = (int) message.getT();
    	}
    	
    	// 尝试压缩消息
    	int msgz = MessageCompressor.doCompress(curTBase, message);
    	
    	if (!MessageCompressor.isValid(msgz)) {
    		
    		// 若压缩失败，可能是因为 偏移太大
    		// 尝试用它自己的 tBase 去压缩
    		msgz = MessageCompressor.doCompress((int)message.getT(), message);
    		
    		if (!MessageCompressor.isValid(msgz)) {
    			// 若还是不能压缩，说明消息不能压缩，转slow path处理
	    		System.out.println(MessageCompressor.dumpMessage(message));
	    		haveUncompressibleRecord = true;
	    		synchronized (uncompressibleRecords) {
	    			uncompressibleRecords.add(message);
	    		}
	    		return;
    		}
    		
    		// 用新偏移压缩成功，新开一个块
    		recordId = (recordId / L_NREC + 1) * L_NREC;
    		curTBase = (int) message.getT();
    		unfullBlocks++;
    	}
    	
    	// 写入存储区
    	long memOffset = recordId * 3L;
    	if (recordId % 1000000 == 0) {
    		System.out.println(String.format("%s: realRecordId=%d recordId=%d unfullBlocks=%d", new Date().toString(), realRecordId, recordId, unfullBlocks));
    	}
    	if (memOffset + 4 > MEMSZ) {
    		System.out.println("ERROR: MEMORY FULL!");
    		System.exit(-1);
    	}
    	unsafe.putInt(memBase + memOffset, msgz);
    	
    	// 若是新的块
    	if (recordId % L_NREC == 0) {
    		int blockId = (int)(recordId / L_NREC);
    		
    		// 登记新块的tBase
    		updateLeafTBase(blockId, curTBase);
    		
    		// 对上一个块计算metadata
    		if (blockId > 0) {
    			updateLeafIndex(blockId - 1);
    		}
    	}
    	recordId++;
    	realRecordId++;
    }
    
    
    private static void doSort(ArrayList<Message> a)
    {
    	Collections.sort(a, new Comparator<Message>() {
			public int compare(Message a, Message b) {
				return Long.compare(a.getT(), b.getT());
			}
		});
    }
    
    private static final int SORTBUFFER_LOW  = 100000;
    private static final int SORTBUFFER_HIGH = 200000;
    private static ArrayList<Message> sortBuffer = new ArrayList<Message>();
    
    @Override
    public synchronized void put(Message message) {
    	if (state == 0) {
			System.out.println("[" + new Date() + "]: put()");
			System.out.println(String.format("memBase=%016X", memBase));
			state = 1;
    	}
    	
    	sortBuffer.add(message);
    	if (sortBuffer.size() >= SORTBUFFER_HIGH) {
    		doSort(sortBuffer);
    		for (int i = 0; i < SORTBUFFER_LOW; i++) {
    			doPutMessage(sortBuffer.get(i));
    		}
    		sortBuffer.subList(0, SORTBUFFER_LOW).clear();
    		
    		System.gc();
    	}
    }

    public void createIndex()
    {
    	// flush sort buffer
    	for (Message m: sortBuffer) {
    		doPutMessage(m);
    	}
    	sortBuffer.clear();
    	sortBuffer = null;
    	
    	// flush last block
    	long nLeaf = recordId;
    	int nBlock = (int)(nLeaf / L_NREC);
    	if (nLeaf % L_NREC != 0) {
    		updateLeafIndex(nBlock);
    		nBlock++;
    	}
    	
    	for (int j = H - 1; j >= 0; j--) {
    		int l = 1 << j;
    		int r = 1 << (j + 1);
    		for (int cur = l; cur < r; cur++) {
    			int cur_base = I_SIZE * (cur);
    			int lch_base = I_SIZE * (cur * 2);
    			int rch_base = I_SIZE * (cur * 2 + 1);
    			
    			indexHeapSet(cur_base + I_MINT, Math.min(indexHeap(lch_base + I_MINT), indexHeap(rch_base + I_MINT))); 
    			indexHeapSet(cur_base + I_MAXT, Math.max(indexHeap(lch_base + I_MAXT), indexHeap(rch_base + I_MAXT)));
    			indexHeapSet(cur_base + I_MINA, Math.min(indexHeap(lch_base + I_MINA), indexHeap(rch_base + I_MINA)));
    			indexHeapSet(cur_base + I_MAXA, Math.max(indexHeap(lch_base + I_MAXA), indexHeap(rch_base + I_MAXA)));
    			
    			long lch_sum = makeLong(indexHeap(lch_base + I_SUMH), indexHeap(lch_base + I_SUML));
    			long rch_sum = makeLong(indexHeap(rch_base + I_SUMH), indexHeap(rch_base + I_SUML));
    			long cur_sum = lch_sum + rch_sum;
    			
    			indexHeapSet(cur_base + I_SUML, (int)(cur_sum & 0xFFFFFFFF));
    			indexHeapSet(cur_base + I_SUMH, (int)(cur_sum >> 32));
    			
    			indexHeapSet(cur_base + I_CNT , indexHeap(lch_base + I_CNT ) + indexHeap(rch_base + I_CNT ));
    		}
    	}
    	
    	System.out.println("nLeaf : " + nLeaf);
    	System.out.println("nBlock: " + nBlock);
    	
    	try {
    		System.out.print(new String(Files.readAllBytes(Paths.get("/proc/meminfo"))));
    	} catch (Exception e) {
    	}
    }
    
    public void doGetMessage(ArrayList<Message> result, int cur, int aMin, int aMax, int tMin, int tMax)
    {
    	
    	if (cur >= HEAP_LEAF_BASE) {
    		int tBase = indexHeap(cur * I_SIZE + I_TBASE);
    		
    		long l = (cur - HEAP_LEAF_BASE) * L_NREC;
    		long r = l + L_NREC;
    		for (long i = l; i < r; i++) {
    			
    			int m = unsafe.getInt(memBase + i * 3);
    			if (MessageCompressor.isValid(m)) {
    				int t = MessageCompressor.extractT(tBase, m);
    				int a = MessageCompressor.extractA(tBase, m);
    				
    				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
    					result.add(MessageCompressor.doDecompress(tBase, m));
    				}
    			}
    		}
    		
    		return;
    	}
    	int lch = cur * 2;
    	int rch = cur * 2 + 1;
		int lch_base = I_SIZE * lch;
		int rch_base = I_SIZE * rch;
		/*System.out.println(String.format("%d lch %d,%d,%d,%d,%d,%d", cur,
				indexHeap(lch_base + I_MINT),
				indexHeap(lch_base + I_MAXT),
				indexHeap(lch_base + I_MINA),
				indexHeap(lch_base + I_MAXA),
				makeLong(indexHeap(lch_base + I_SUMH), indexHeap(lch_base + I_SUML)),
				indexHeap(lch_base + I_CNT )
		));
		System.out.println(String.format("%d rch %d,%d,%d,%d,%d,%d", cur,
				indexHeap(rch_base + I_MINT),
				indexHeap(rch_base + I_MAXT),
				indexHeap(rch_base + I_MINA),
				indexHeap(rch_base + I_MAXA),
				makeLong(indexHeap(rch_base + I_SUMH), indexHeap(rch_base + I_SUML)),
				indexHeap(rch_base + I_CNT )
		));*/
		
		if (rectOverlap(
				indexHeap(lch_base + I_MINT), indexHeap(lch_base + I_MAXT),
				indexHeap(lch_base + I_MINA), indexHeap(lch_base + I_MAXA),
				tMin, tMax,
				aMin, aMax)) {
			doGetMessage(result, lch, aMin, aMax, tMin, tMax);
		}
		if (rectOverlap(
				indexHeap(rch_base + I_MINT), indexHeap(rch_base + I_MAXT),
				indexHeap(rch_base + I_MINA), indexHeap(rch_base + I_MAXA),
				tMin, tMax,
				aMin, aMax)) {
			doGetMessage(result, rch, aMin, aMax, tMin, tMax);
		}
    }
    
    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

    	if (state == 1) {
    		synchronized (stateLock) {
				if (state == 1) {
					System.out.println("[" + new Date() + "]: createIndex()");
					createIndex();
					System.out.println("unfullBlocks=" + unfullBlocks);
					System.out.println("uncompressableRecords = " + uncompressibleRecords.size());
					System.out.println("[" + new Date() + "]: getMessage()");
					state = 2;
				}
    		}
    	}
    	
    	System.gc();
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	doGetMessage(result, 1, (int)aMin, (int)aMax, (int)tMin, (int)tMax);

    	if (haveUncompressibleRecord) {
    		for (Message msg: uncompressibleRecords) {
    			if (pointInRectL(msg.getT(), msg.getA(), tMin, tMax, aMin, aMax)) {
					result.add(msg);
				}
    		}
    	}
    	
    	doSort(result);
    	
    	// 预热
    	getAvgValue(aMin, aMax, tMin, tMax);

    	return result;
    }


    
    class AvgResult {
    	long sum;
    	int cnt;
    }
    
    public void doGetAvgValue(AvgResult result, int cur, int aMin, int aMax, int tMin, int tMax)
    {
    	
    	if (cur >= HEAP_LEAF_BASE) {
    		
    		int tBase = indexHeap(cur * I_SIZE + I_TBASE);
    		
    		long l = (cur - HEAP_LEAF_BASE) * L_NREC;
    		long r = l + L_NREC;
    		
    		for (long i = l; i < r; i++) {
    			
        		
    			//int m = unsafe.getInt(memBase + i * 3);
    			int s0 = ((int)unsafe.getShort(memBase + i * 3) & 0xFFFF);
    			
    			//if (MessageCompressor.isValid(m)) {
    			if (s0 == 0) break;
    			
				//int t = MessageCompressor.extractT(tBase, m);
				//int a = MessageCompressor.extractA(tBase, m);
				int t = tBase + ((int)unsafe.getByte(memBase + i * 3 + 2) & 0xFF);
        		int a = tBase + s0 - 10000;
        		
				
				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
					result.sum += a;
					result.cnt++;
				}
    		}
    		
    		return;
    	}
    	
    	int lch = cur * 2;
    	int rch = cur * 2 + 1;
		int lch_base = I_SIZE * lch;
		int rch_base = I_SIZE * rch;
		
		if (rectInRect(
				indexHeap(lch_base + I_MINT), indexHeap(lch_base + I_MAXT),
				indexHeap(lch_base + I_MINA), indexHeap(lch_base + I_MAXA),
				tMin, tMax,
				aMin, aMax)) {
			result.sum += indexHeapL(lch_base + I_SUML);
			result.cnt += indexHeap(lch_base + I_CNT );
		} else if (rectOverlap(
					indexHeap(lch_base + I_MINT), indexHeap(lch_base + I_MAXT),
					indexHeap(lch_base + I_MINA), indexHeap(lch_base + I_MAXA),
					tMin, tMax,
					aMin, aMax)) {
			doGetAvgValue(result, lch, aMin, aMax, tMin, tMax);
		}
		
		if (rectInRect(
				indexHeap(rch_base + I_MINT), indexHeap(rch_base + I_MAXT),
				indexHeap(rch_base + I_MINA), indexHeap(rch_base + I_MAXA),
				tMin, tMax,
				aMin, aMax)) {
			result.sum += indexHeapL(rch_base + I_SUML);
			result.cnt += indexHeap(rch_base + I_CNT );
		} else if (rectOverlap(
					indexHeap(rch_base + I_MINT), indexHeap(rch_base + I_MAXT),
					indexHeap(rch_base + I_MINA), indexHeap(rch_base + I_MAXA),
					tMin, tMax,
					aMin, aMax)) {
			doGetAvgValue(result, rch, aMin, aMax, tMin, tMax);
		}
    }
    
    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {

    	AvgResult result = new AvgResult();
    	doGetAvgValue(result, 1, (int)aMin, (int)aMax, (int)tMin, (int)tMax);
    	
    	if (haveUncompressibleRecord) {
    		for (Message msg: uncompressibleRecords) {
    			if (pointInRectL(msg.getT(), msg.getA(), tMin, tMax, aMin, aMax)) {
					result.sum += msg.getA();
					result.cnt++;
				}
    		}
    	}
    	
    	return result.cnt == 0 ? 0 : result.sum / result.cnt;
    }

}
