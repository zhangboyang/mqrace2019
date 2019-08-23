package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
    	Collections.sort(a, new Comparator<Message>() {
			public int compare(Message a, Message b) {
				return Long.compare(a.getT(), b.getT());
			}
		});
    }
    
    
    private static volatile int state = 0;
    private static final Object stateLock = new Object();
    
    static {
    	Runtime.getRuntime().addShutdownHook(new Thread() {
    		public void run() {
    			System.out.println("[" + new Date() + "]: shutdown hook");
    		}
    	});
    }
    



    
    private static final Object insertLock = new Object();
    private static int insCount = 0;
    
    private static Message[] msgBuffer = new Message[10000000];
    private static int msgBufferPtr = 0;
    
    private static void flushMsgBuffer()
    {
    	for (int i = 0; i < msgBufferPtr; i++) {
			RTree.insert(msgBuffer[i]);
		}
		msgBufferPtr = 0;
    }
    
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
    
    	synchronized (insertLock) {
    		msgBuffer[msgBufferPtr++] = message;
    		
			if (insCount % 1000000 == 0) {
				System.out.println(String.format("insert %d: %s", insCount, dumpMessage(message)));
			}
			insCount++;
			
    		if (msgBufferPtr == msgBuffer.length) {
    			flushMsgBuffer();
    		}
    	}
    	
    }
    
    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

//    	boolean firstFlag = false;
    	
    	if (state == 1) {
    		synchronized (stateLock) {
    			if (state == 1) {
    				System.out.println("[" + new Date() + "]: getMessage() started");
    				
    				flushMsgBuffer();
    				msgBuffer = null;
    				RTree.finishInsert();
    				System.gc();
    				
//    				firstFlag = true;
    				state = 2;
    			}
    		}
    	}
    	
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	RTree.queryData(result, tMin, tMax, aMin, aMax);
    	doSortMessage(result);
    	
//    	for (int i = 0; i < result.size(); i++) {
//    		System.out.println("RESULT:" + dumpMessage(result.get(i)));
//    	}
    	

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
    	return RTree.queryAverage(tMin, tMax, aMin, aMax);
    }
}
