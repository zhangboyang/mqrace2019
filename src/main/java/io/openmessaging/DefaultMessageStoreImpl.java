package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
    

    private static int insCount = 0;

    private static boolean insertDone = false;
    
    private static void flushInsertQueue()
    {
    	insertDone = true;
    	int nThread = putThreadCount.get();
    	try {
	    	for (int i = 0; i < nThread; i++) {
	    		insertQueue.put(putTLD[i].buffer);
	    		putTLD[i] = null;
	    	}
	    } catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    private static void insertProc()
    {
    	System.out.println("[" + new Date() + "]: insert thread started");
    	
    	try {
	    	do {
	    		Message[] buffer = insertQueue.take();
	    		for (int i = 0; i < buffer.length; i++) {
	    			if (buffer[i] != null) {
	    				
	    				RTree.insert(buffer[i]);
	    				if (insCount % 1000000 == 0) {
	    					System.out.println("[" + new Date() + "]: " + String.format("ins %d (height %d): %s", insCount, RTree.treeHeight, dumpMessage(buffer[i])));
	    				}
	    				insCount++;
	    			}
	    		}
	    	} while (!insertDone || !insertQueue.isEmpty());
	    } catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
    	System.out.println("[" + new Date() + "]: insert thread finished");
    }

    
    private static final int MAXTHREAD = 100;
    private static final int MAXQUEUE = 20;
    
    private static final ArrayBlockingQueue<Message[]> insertQueue = new ArrayBlockingQueue<Message[]>(MAXQUEUE);
    

    
    
    private static final int MAXBUFFER = 100;
    private static class PutThreadLocalData {
    	Message[] buffer;
    	int bufptr;
    	
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
        	putTLD[putThreadCount.getAndIncrement()] = pd;
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
					sortThread = new Thread() {
					    public void run() {
					    	try {
								Thread.sleep(3000);
							} catch (InterruptedException e) {
								e.printStackTrace();
								System.exit(-1);
							}
					    	insertProc();
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
				insertQueue.put(pd.buffer);
				pd.clear();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

//    	boolean firstFlag = false;
    	
    	if (state == 1) {
    		synchronized (stateLock) {
    			if (state == 1) {
    				System.out.println("[" + new Date() + "]: getMessage() started");
    				
    				flushInsertQueue();
    				
    				try {
						sortThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(-1);
					}
    				
    				System.out.println(String.format("total=%d", insCount));
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
