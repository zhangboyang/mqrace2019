package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    		buffer.putLong(0, a);
    		return buffer.array();
    	}
    	public static long doCompress(Message message)
    	{
    		/*if (ThreadLocalRandom.current().nextInt(1000) == 0) {
    			return 0;
    		} else {
    			return makeLong((int)message.getT(), (int)message.getA()) | (1L << 63);
    		}*/
    		long t = message.getT();
    		long a = message.getA();
    		if (t > 0 && Arrays.equals(getBody(t, a), message.getBody())) {
    			return makeLong((int)t, (int)a);
    		} else {
    			return 0;
    		}
    	}
    	public static boolean isValid(long m)
    	{
    		//return m < 0;
    		return m != 0;
    	}
    	public static int extractT(long c)
    	{
    		return (int)(c >> 32) & 0x7FFFFFFF;
    	}
    	public static int extractA(long c)
    	{
    		return (int)(c & 0xFFFFFFFF);
    	}
    	public static Message doDecompress(long m)
    	{
    		/*ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(0, extractT(m));
    		return new Message(extractT(m), extractA(m), buffer.array());*/
    		int t = extractT(m);
    		int a = extractA(m);
    		return new Message(t, a, getBody(t, a));
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
    private static final Method map0;
    
    private static final String MAPFILE = "/alidata1/race2019/data/mydata";
    //private static final String MAPFILE = "storage.dat";
    private static final long MAPLEN = 4 * 1048576 * 4096L;
    private static final long mapBase;
    
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
        
        Method theMap0;
        try {
        	theMap0 = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
        	theMap0.setAccessible(true);
        } catch (Exception e) {
        	theMap0 = null;
        }
        map0 = theMap0;
        
		long theBase;
		try {
			final RandomAccessFile backingFile = new RandomAccessFile(MAPFILE, "rw");
			backingFile.setLength(MAPLEN);
			final FileChannel ch = backingFile.getChannel();
			theBase = (long) map0.invoke(ch, 1, 0L, MAPLEN);
		} catch (Exception e) {
			e.printStackTrace();
			theBase = 0;
			System.exit(-1);
		}
		mapBase = theBase;
    }
    
    
    
    private static final int I_SIZE = 8; // index-record size
    
    private static final int I_MINT = 0;
    private static final int I_MAXT = 1;
    private static final int I_MINA = 2;
    private static final int I_MAXA = 3;
    private static final int I_SUML = 4;
    private static final int I_SUMH = 5;
    private static final int I_CNT  = 6;
    
    private static final int H = 22; // max height of HEAP
    private static final int HEAP_ARRAY_SIZE = ((1 << (H + 1)) + 1);
    private static int indexHeap[] = new int[HEAP_ARRAY_SIZE * I_SIZE];
    private static final int HEAP_LEAF_BASE = 1 << H;
    
    private static AtomicIntegerArray blockCounter = new AtomicIntegerArray(1 << H);
    
    private static final int L_PGSZ = 4096; // leaf-record block size
    private static final int L_NREC = L_PGSZ / 8; // n-record in one block
    

    private static final AtomicLong nextLeafId = new AtomicLong(0);
    //private static long leafStorage[] = new long[10000000];
    
    private static volatile int state = 0;
    private static Object stateLock = new Object();
    
    private static boolean haveUncompressibleRecord = false;
    private static ArrayList<Message> uncompressibleRecords = new ArrayList<Message>();
    
    private static void updateLeafIndex(int leafBlockId)
    {
    	int minT = Integer.MAX_VALUE;
		int maxT = Integer.MIN_VALUE;
		int minA = Integer.MAX_VALUE;
		int maxA = Integer.MIN_VALUE;
		long sumA = 0;
		int cnt  = 0;
		
		long l = (long)leafBlockId * L_NREC;
		long r = l + L_NREC;
		for (long i = l; i < r; i++) {
			//long m = leafStorage[(int)i];
			long m = unsafe.getLong(mapBase + i * 8);
			
			if (MessageCompressor.isValid(m)) {
				int t = MessageCompressor.extractT(m);
				int a = MessageCompressor.extractA(m);
				
				minT = Math.min(minT, t);
				maxT = Math.max(maxT, t);
				minA = Math.min(minA, a);
				maxA = Math.max(maxA, a);
				
				sumA += a;
				cnt++;
			}
		}
		
		int base = (HEAP_LEAF_BASE + leafBlockId) * I_SIZE;
		indexHeap[base + I_MINT] = minT; 
		indexHeap[base + I_MAXT] = maxT;
		indexHeap[base + I_MINA] = minA;
		indexHeap[base + I_MAXA] = maxA;
		indexHeap[base + I_SUML] = (int)(sumA & 0xFFFFFFFF);
		indexHeap[base + I_SUMH] = (int)(sumA >> 32);
		indexHeap[base + I_CNT ] = cnt;
		
    }
    
    @Override
    public void put(Message message) {
    	
    	if (state == 0) {
    		synchronized (stateLock) {
    			if (state == 0) {
    				System.out.println("[" + new Date() + "]: put()");
    				System.out.println(String.format("mapBase=%016X", mapBase));
    				state = 1;
    			}
    		}
    	}
    	
    	
    	long msgz = MessageCompressor.doCompress(message);
    	
    	if (!MessageCompressor.isValid(msgz)) {
    		System.out.println(MessageCompressor.dumpMessage(message));
    		haveUncompressibleRecord = true;
    		synchronized (uncompressibleRecords) {
    			uncompressibleRecords.add(message);
    		}
    		return;
    	}
    	
    	long id = nextLeafId.getAndIncrement();
    	//leafStorage[(int)id] = msgz;
    	unsafe.putLong(mapBase + id * 8, msgz);
    	
    	int blkid = (int)(id / L_NREC);
    	if (blockCounter.incrementAndGet(blkid) == L_NREC) {
    		updateLeafIndex(blkid);
    	}
    }

    public void createIndex()
    {
    	blockCounter = null;
    	
    	long nLeaf = nextLeafId.get();
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
    			
    			indexHeap[cur_base + I_MINT] = Math.min(indexHeap[lch_base + I_MINT], indexHeap[rch_base + I_MINT]); 
    			indexHeap[cur_base + I_MAXT] = Math.max(indexHeap[lch_base + I_MAXT], indexHeap[rch_base + I_MAXT]);
    			indexHeap[cur_base + I_MINA] = Math.min(indexHeap[lch_base + I_MINA], indexHeap[rch_base + I_MINA]);
    			indexHeap[cur_base + I_MAXA] = Math.max(indexHeap[lch_base + I_MAXA], indexHeap[rch_base + I_MAXA]);
    			
    			long lch_sum = makeLong(indexHeap[lch_base + I_SUMH], indexHeap[lch_base + I_SUML]);
    			long rch_sum = makeLong(indexHeap[rch_base + I_SUMH], indexHeap[rch_base + I_SUML]);
    			long cur_sum = lch_sum + rch_sum;
    			
    			indexHeap[cur_base + I_SUML] = (int)(cur_sum & 0xFFFFFFFF);
    			indexHeap[cur_base + I_SUMH] = (int)(cur_sum >> 32);
    			
    			indexHeap[cur_base + I_CNT ] = indexHeap[lch_base + I_CNT ] + indexHeap[rch_base + I_CNT ];
    		}
    	}
    	
    	System.out.println("nLeaf : " + nLeaf);
    	System.out.println("nBlock: " + nBlock);
    }
    
    public void doGetMessage(ArrayList<Message> result, int cur, int aMin, int aMax, int tMin, int tMax)
    {
    	
    	if (cur >= HEAP_LEAF_BASE) {
    		
    		long l = (cur - HEAP_LEAF_BASE) * L_NREC;
    		long r = l + L_NREC;
    		for (long i = l; i < r; i++) {
    			//long m = leafStorage[(int)i];
    			long m = unsafe.getLong(mapBase + i * 8);
    			if (MessageCompressor.isValid(m)) {
    				int t = MessageCompressor.extractT(m);
    				int a = MessageCompressor.extractA(m);
    				
    				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
    					result.add(MessageCompressor.doDecompress(m));
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
				indexHeap[lch_base + I_MINT],
				indexHeap[lch_base + I_MAXT],
				indexHeap[lch_base + I_MINA],
				indexHeap[lch_base + I_MAXA],
				makeLong(indexHeap[lch_base + I_SUMH], indexHeap[lch_base + I_SUML]),
				indexHeap[lch_base + I_CNT ]
		));
		System.out.println(String.format("%d rch %d,%d,%d,%d,%d,%d", cur,
				indexHeap[rch_base + I_MINT],
				indexHeap[rch_base + I_MAXT],
				indexHeap[rch_base + I_MINA],
				indexHeap[rch_base + I_MAXA],
				makeLong(indexHeap[rch_base + I_SUMH], indexHeap[rch_base + I_SUML]),
				indexHeap[rch_base + I_CNT ]
		));*/
		
		if (rectOverlap(
				indexHeap[lch_base + I_MINT], indexHeap[lch_base + I_MAXT],
				indexHeap[lch_base + I_MINA], indexHeap[lch_base + I_MAXA],
				tMin, tMax,
				aMin, aMax)) {
			doGetMessage(result, lch, aMin, aMax, tMin, tMax);
		}
		if (rectOverlap(
				indexHeap[rch_base + I_MINT], indexHeap[rch_base + I_MAXT],
				indexHeap[rch_base + I_MINA], indexHeap[rch_base + I_MAXA],
				tMin, tMax,
				aMin, aMax)) {
			doGetMessage(result, rch, aMin, aMax, tMin, tMax);
		}
    }
    
    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	
    	if (state == 1) {
    		synchronized (stateLock) {
    			if (state == 1) {
    				System.out.println("[" + new Date() + "]: createIndex()");
    				createIndex();
    				System.out.println("uncompressableRecords = " + uncompressibleRecords.size());
    				System.out.println("[" + new Date() + "]: getMessage()");
    				state = 2;
    			}
    		}
    	}
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	doGetMessage(result, 1, (int)aMin, (int)aMax, (int)tMin, (int)tMax);

    	if (haveUncompressibleRecord) {
    		for (Message msg: uncompressibleRecords) {
    			if (pointInRectL(msg.getT(), msg.getA(), tMin, tMax, aMin, aMax)) {
					result.add(msg);
				}
    		}
    	}
    	
    	Collections.sort(result, new Comparator<Message>() {
			public int compare(Message a, Message b) {
				return Long.compare(a.getT(), b.getT());
			}
		});
    	
    	return result;
    }


    
    class AvgResult {
    	long sum;
    	int cnt;
    }
    public void doGetAvgValue(AvgResult result, int cur, int aMin, int aMax, int tMin, int tMax)
    {
    	
    	if (cur >= HEAP_LEAF_BASE) {
    		
    		long l = (cur - HEAP_LEAF_BASE) * L_NREC;
    		long r = l + L_NREC;
    		for (long i = l; i < r; i++) {
    			//long m = leafStorage[(int)i];
    			long m = unsafe.getLong(mapBase + i * 8);
    			if (MessageCompressor.isValid(m)) {
    				int t = MessageCompressor.extractT(m);
    				int a = MessageCompressor.extractA(m);
    				
    				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
    					result.sum += a;
    					result.cnt++;
    				}
    			}
    		}
    		
    		return;
    	}
    	
    	int lch = cur * 2;
    	int rch = cur * 2 + 1;
		int lch_base = I_SIZE * lch;
		int rch_base = I_SIZE * rch;
		
		if (rectInRect(
				indexHeap[lch_base + I_MINT], indexHeap[lch_base + I_MAXT],
				indexHeap[lch_base + I_MINA], indexHeap[lch_base + I_MAXA],
				tMin, tMax,
				aMin, aMax)) {
			result.sum += makeLong(indexHeap[lch_base + I_SUMH], indexHeap[lch_base + I_SUML]);
			result.cnt += indexHeap[lch_base + I_CNT ];
		} else if (rectOverlap(
					indexHeap[lch_base + I_MINT], indexHeap[lch_base + I_MAXT],
					indexHeap[lch_base + I_MINA], indexHeap[lch_base + I_MAXA],
					tMin, tMax,
					aMin, aMax)) {
			doGetAvgValue(result, lch, aMin, aMax, tMin, tMax);
		}
		
		if (rectInRect(
				indexHeap[rch_base + I_MINT], indexHeap[rch_base + I_MAXT],
				indexHeap[rch_base + I_MINA], indexHeap[rch_base + I_MAXA],
				tMin, tMax,
				aMin, aMax)) {
			result.sum += makeLong(indexHeap[rch_base + I_SUMH], indexHeap[rch_base + I_SUML]);
			result.cnt += indexHeap[rch_base + I_CNT ];
		} else if (rectOverlap(
					indexHeap[rch_base + I_MINT], indexHeap[rch_base + I_MAXT],
					indexHeap[rch_base + I_MINA], indexHeap[rch_base + I_MAXA],
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
