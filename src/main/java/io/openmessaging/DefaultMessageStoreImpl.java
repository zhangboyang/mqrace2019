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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import sun.nio.ch.FileChannelImpl;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

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
	
    private static void doSortMessage(ArrayList<Message> a)
    {
    	Collections.sort(a, new Comparator<Message>() {
			public int compare(Message a, Message b) {
				return Long.compare(a.getT(), b.getT());
			}
		});
    }
    
	
	private static final int MAX_MSGBUF = 1000;
	private static final int MESSAGE_SIZE = 50;
	
	private static Message deserializeMessage(ByteBuffer buffer, int position)
	{
		byte body[] = new byte[34];
		long t = buffer.getLong(position + 0);
		long a = buffer.getLong(position + 8);
		System.arraycopy(buffer.array(), position + 16, body, 0, body.length);
		return new Message(a, t, body); 
	}
	

	private static boolean rectOverlap(long aLeft, long aRight, long aBottom, long aTop, long bLeft, long bRight, long bBottom, long bTop)
	{
		return aLeft <= bRight && aRight >= bLeft && aTop >= bBottom && aBottom <= bTop;
	}
	
	private static boolean pointInRect(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop)
	{
		return rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop;
	}
	
	private static boolean rectInRect(long aLeft, long aRight, long aBottom, long aTop, long bLeft, long bRight, long bBottom, long bTop)
	{
		return bLeft <= aLeft && aRight <= bRight && bBottom <= aBottom && aTop <= bTop;
	}
    
    private static final Unsafe unsafe;

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
    }
    
    private static final String storagePath = "./";
//    private static final String storagePath = "/alidata1/race2019/data/";
    
    private static final long MEMSZ = 30000000L * 3;
//    private static final long MEMSZ = 2100000000L * 3;
    private static final long memBase;
    
    static {
        memBase = unsafe.allocateMemory(MEMSZ);
        unsafe.setMemory(memBase, MEMSZ, (byte)0);
        System.out.println(String.format("memBase=%016X", memBase));
    }

    private static volatile int state = 0;
    private static final Object stateLock = new Object();

    private static final int MAXTHREAD = 100;
    
    @Override
    public void put(Message message) {
    	
    	if (state == 0) {
    		synchronized (stateLock) {
    			if (state == 0) {
					System.out.println("[" + new Date() + "]: put() started");
					state = 1;
    			}
    		}
    	}
    	
    	if (ThreadLocalRandom.current().nextInt(210000) == 0) {
    		System.out.println(dumpMessage(message));
    	}
    	
    }
    
    private static final Object getMessageLock = new Object(); 
    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

    	boolean firstFlag = false;
    	ArrayList<Message> result;
    	
    	synchronized (getMessageLock) {
	    	if (state == 1) {
	    		System.out.println("[" + new Date() + "]: getMessage() started");

				state = 2;
				firstFlag = true;
	    	}
	    	
	    	System.gc();
	    	
	    	result = new ArrayList<Message>();
	    	

    	}
    	
    	// 为最后的查询平均值预热JVM
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

    
    
    
    
    
    ////////////////////////////////////////////////////////////////
    /////////////////////// SNAPSHOT ///////////////////////////////
    ////////////////////////////////////////////////////////////////
    private static void loadMemory(String fn, long base, long len) throws IOException
    {
    	if (unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
    		System.out.println("ERROR: Unsafe.ARRAY_BYTE_INDEX_SCALE != 1");
    		System.exit(-1);
    	}
    	
		byte buf[] = new byte[4096];
		RandomAccessFile f = new RandomAccessFile(storagePath + fn, "r");
		
		while (len > 0) {
			int rlen = (int) Math.min((long)buf.length, len);
			
			f.readFully(buf, 0, rlen);
			unsafe.copyMemory(buf, unsafe.ARRAY_BYTE_BASE_OFFSET, null, base, rlen);

			base += rlen;
			len -= rlen;
		}
    }
    
    private static void saveMemory(String fn, long base, long len)
    {
    	if (unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
    		System.out.println("ERROR: Unsafe.ARRAY_BYTE_INDEX_SCALE != 1");
    		System.exit(-1);
    	}
    	
    	try {
    		byte buf[] = new byte[4096];
			RandomAccessFile f = new RandomAccessFile(storagePath + fn, "rw");
			f.setLength(0);
			
			while (len > 0) {
				int wlen = (int) Math.min((long)buf.length, len);
				
				unsafe.copyMemory(null, base, buf, unsafe.ARRAY_BYTE_BASE_OFFSET, wlen);
				f.write(buf, 0, wlen);
				
				base += wlen;
				len -= wlen;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    public static boolean loadSnapshot()
    {
    	System.out.println("[" + new Date().toString() + "]: LOADING SNAPSHOT ...");
    	try {
    		loadMemory("snapshot.mem.data", memBase, MEMSZ);
    		System.out.println("[" + new Date().toString() + "]: SNAPSHOT LOADED!");
    		return true;
    	} catch (IOException e) {
    		System.out.println("[" + new Date().toString() + "]: ERROR LOADING SNAPSHOT!");
    		return false;
    	}
    }
    
    public static void saveSnapshot()
    {
    	System.out.println("[" + new Date().toString() + "]: SAVING SNAPSHOT ...");
    	saveMemory("snapshot.mem.data", memBase, MEMSZ);
    	System.out.println("[" + new Date().toString() + "]: SNAPSHOT SAVED!");
    }
}
