package io.openmessaging;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    //private NavigableMap<Long, List<Message>> msgMap = new TreeMap<Long, List<Message>>();
	
    // Atomic integer containing the next thread ID to be assigned
    private static final AtomicInteger nextId = new AtomicInteger(0);

    // Thread local variable containing each thread's ID
    private static final ThreadLocal<Integer> threadId =
        new ThreadLocal<Integer>() {
            @Override protected Integer initialValue() {
                return nextId.getAndIncrement();
        }
    };
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    
    
    private static long maxdiff = Long.MIN_VALUE;
    private static long mindiff = Long.MAX_VALUE;
    private static long sumdiff = 0;
    private static boolean flag = false;
    private static long n = 0;
    private static int maxthread = 0;
    
    private static long lastThreadT[] = new long[100];
    
    private static long minDiffT[] = new long[100];
    private static long minDiffTGlobal = Long.MAX_VALUE;
    {
    	Arrays.fill(minDiffT, Long.MAX_VALUE);
    }
    
    @Override
    public synchronized void put(Message message) {
        /*if (!msgMap.containsKey(message.getT())) {
            msgMap.put(message.getT(), new ArrayList<Message>());
        }

        msgMap.get(message.getT()).add(message);*/
    	int tid = threadId.get();
    	
    	long diff = message.getT() - message.getA();
    	if (diff > maxdiff) maxdiff = diff;
    	if (diff < mindiff) mindiff = diff;
    	n++;
    	sumdiff += Math.abs(diff);
    	
    	long diffT = message.getT() - lastThreadT[tid];
    	lastThreadT[tid] = message.getT();
    	minDiffT[tid] = Math.min(minDiffT[tid], diffT);
    	minDiffTGlobal = Math.min(minDiffTGlobal, diffT);
    	
    	
    	if (tid + 1 > maxthread) maxthread = tid + 1;
    	
    	
    	if (ThreadLocalRandom.current().nextInt(10000) == 0) {
	    	StringBuilder s = new StringBuilder();
	    	s.append(String.format("%04d,", tid));
	    	s.append(String.format("%08X,", message.getT()));
	    	s.append(String.format("%08X,", message.getA()));
	    	byte[] bytes = message.getBody();
	    	for (int j = 0; j < bytes.length; j++) {
	            int v = bytes[j] & 0xFF;
	            s.append(HEX_ARRAY[v >>> 4]);
	            s.append(HEX_ARRAY[v & 0x0F]);
	        }
	    	System.out.println(s);
    	}
    }


    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        /*ArrayList<Message> res = new ArrayList<Message>();
        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
            List<Message> msgQueue = mapEntry.getValue();
            for (Message msg : msgQueue) {
                if (msg.getA() >= aMin && msg.getA() <= aMax) {
                    res.add(msg);
                }
            }
        }

        return res;*/
    	if (!flag) {
    		flag = true;
    		
    		System.out.println("maxdiff: " + maxdiff);
    		System.out.println("mindiff: " + mindiff);
    		System.out.println("N: " + n);
    		System.out.println("avgDiff: " + (sumdiff / n));
    		System.out.println("maxthread: " + maxthread);
    		System.out.println("minDiffTGlobal: " + minDiffTGlobal);
    		for (int i = 0; i < maxthread; i++) {
    			System.out.println("minDiffT: " + minDiffT[i]);
    		}
    	}
    	return new ArrayList<Message>();
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        /*long sum = 0;
        long count = 0;
        NavigableMap<Long, List<Message>> subMap = msgMap.subMap(tMin, true, tMax, true);
        for (Map.Entry<Long, List<Message>> mapEntry : subMap.entrySet()) {
            List<Message> msgQueue = mapEntry.getValue();
            for (Message msg : msgQueue) {
                if (msg.getA() >= aMin && msg.getA() <= aMax) {
                    sum += msg.getA();
                    count++;
                }
            }
        }

        return count == 0 ? 0 : sum / count;*/
    	return 0;
    }

}
