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
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.ReentrantLock;

import io.openmessaging.RTree.AverageResult;
import io.openmessaging.RTree.NodeEntry;
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
    			atShutdown();
    		}
    	});
    }
    

    private static int MAXMSG = 2100000000;
    private static int MAXSLICE = 2000;
    
    private static int nSlice = 1;
    private static long slicePivot[] = new long[MAXSLICE];
    private static NodeEntry sliceRoot[] = new NodeEntry[MAXSLICE];
    
    static {
    	for (int i = 0; i < MAXSLICE; i++) {
    		sliceRoot[i] = RTree.allocRootNode();
    	}
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
    		long maxT = Long.MIN_VALUE;
    		
	    	do {
	    		Message[] buffer = insertQueue.take();
	    		for (int i = 0; i < buffer.length; i++) {
	    			if (buffer[i] != null) {
	    				
	    				if (insCount % 1000000 == 0) {
	    					System.out.println("[" + new Date() + "]: " + String.format("ins %d: %s", insCount, dumpMessage(buffer[i])));
	    				}
	    				
	    				long t = buffer[i].getT();
	    				maxT = Math.max(maxT, t);
	    				
	    				
	    				insCount++;
	    				if (insCount % (MAXMSG / MAXSLICE) == 0) {
	    					slicePivot[nSlice++] = maxT;
	    				}
	    				
	    				int l = 0, r = nSlice;
	    				while (r - l > 1) {
	    					int m = (l + r) / 2;
	    					if (t >= slicePivot[m]) {
	    						l = m;
	    					} else {
	    						r = m;
	    					}
	    				}
	    				
	    				assert t >= slicePivot[l];
	    				assert r == nSlice || t < slicePivot[r];
	    				
	    				sliceRoot[l] = RTree.insertToTree(sliceRoot[l], buffer[i]);

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
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {   	

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
    				
    				for (int i = 0; i < nSlice; i++) {
    		    		NodeEntry r = sliceRoot[i];
    		    		System.out.println(String.format("slice %d: pivot=%d tree=(%d,%d,%d,%d)(%d,%d)", i, slicePivot[i], r.left, r.right, r.bottom, r.top, r.sumA, r.cntA));
    		    	}
    				
//    				firstFlag = true;
    				state = 2;
    			}
    		}
    	}
    	
    	
    	
    	System.out.println("[" + new Date() + "]: " + String.format("queryData: %d %d %d %d", tMin, tMax, aMin, aMax));
    	
    	ArrayList<Message> result = new ArrayList<Message>();
    	
    	for (int i = 0; i < nSlice; i++) {
    		RTree.queryData(sliceRoot[i], result, tMin, tMax, aMin, aMax);
    	}

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

    private static final LongAccumulator nAvgStartTime = new LongAccumulator(Math::min, Long.MAX_VALUE);
    private static final AtomicLong nAvgResult = new AtomicLong();
    private static final AtomicLong nAvgQuery = new AtomicLong();
    private static final AtomicLong nAvgQueryLeaf = new AtomicLong();
    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
    	nAvgStartTime.accumulate(System.nanoTime());
    	
    	AverageResult result = new AverageResult();
    	
    	int nValidSlice = 0;
    	
    	for (int i = 0; i < nSlice; i++) {
    		NodeEntry r = sliceRoot[i];
    		if (RTree.rectOverlap(r.left, r.right, r.bottom, r.top, tMin, tMax, aMin, aMax)) {
    			RTree.queryAverage(r, result, tMin, tMax, aMin, aMax);
    			nValidSlice++;
    		}
    	}
    	
    	System.out.println("[" + new Date() + "]: " + String.format("queryAverage: nValidSlice=%d nLeaf=%d (%d %d %d %d)", nValidSlice, result.nleaf, tMin, tMax, aMin, aMax));
    	
    	nAvgQuery.incrementAndGet();
    	nAvgQueryLeaf.addAndGet(result.nleaf);
    	nAvgResult.addAndGet(result.cnt);
    	
    	return result.cnt > 0 ? result.sum / result.cnt : 0;
    }
    
    private static void atShutdown()
    {
    	long deltaT = System.nanoTime() - nAvgStartTime.get();
    	System.out.println("[" + new Date() + "]: shutdown hook");
    	
    	System.out.println(String.format("deltaT=%f", deltaT * 1e-6));
    	System.out.println(String.format("Result=%d", nAvgResult.get()));
    	System.out.println(String.format("expectedScore=%f", nAvgResult.get() * 1e6 / deltaT));
    	System.out.println(String.format("Query=%d", nAvgQuery.get()));
    	System.out.println(String.format("QueryLeaf=%d", nAvgQueryLeaf.get()));
    	System.out.println(String.format("leaf/query=%f", (double)nAvgQueryLeaf.get() / nAvgQuery.get()));
    }
}
