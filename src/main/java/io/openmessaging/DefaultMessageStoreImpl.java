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
    
    @Override
    public synchronized void put(Message message) {
        /*if (!msgMap.containsKey(message.getT())) {
            msgMap.put(message.getT(), new ArrayList<Message>());
        }

        msgMap.get(message.getT()).add(message);*/
    	
    	if (ThreadLocalRandom.current().nextInt(10000) == 0) {
	    	StringBuilder s = new StringBuilder();
	    	s.append(threadId.get());
	    	s.append(',');
	    	s.append(Long.toHexString(message.getA()));
	    	s.append(',');
	    	s.append(Long.toHexString(message.getT()));
	    	s.append(',');
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
