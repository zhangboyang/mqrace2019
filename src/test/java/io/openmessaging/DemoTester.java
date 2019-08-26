package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;

import java.util.concurrent.atomic.AtomicLong;


//这是评测程序的一个demo版本，其评测逻辑与实际评测程序基本类似，但是比实际评测简单很多
//该评测程序主要便于选手在本地优化和调试自己的程序

public class DemoTester {
	static int TESTMODE = 1;
	
	
	static long genAfromT(long T)
	{
		if (TESTMODE == 1) {
			return T % 2 == 0 ? T + 30000 : T - 500;
		}
		if (TESTMODE == 2) {
			long x = 100003;
			long r = 1;
			while (T > 0) {
				if ((T & 1) != 0) {
					r *= x;
				}
				T = T >>> 1;
				x = x * x;
			}
			return r & 0xffffffffffffL;
		}
		return T;
	}
	
    public static void main(String args[]) throws Exception {
        //评测相关配置
        //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
        int msgNum = 13340000;
        //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
        int sendTime = 600 * 60 * 1000;
        //查询阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
        int checkTime = 60 * 60 * 1000;

        //正确性检测的次数
        int getMessageTimes = 30;
        int checkTimes = 30652;
        //发送的线程数量
        int sendTsNum = 10;
        //查询的线程数量
        int checkTsNum = 40;
        // 每次查询消息的最大跨度
        int maxMsgCheckSize = 100000;
        // 每次查询求平均的最大跨度
        int maxValueCheckSize = 1000000;

        DefaultMessageStoreImpl messageStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultMessageStoreImpl");
            messageStore = (DefaultMessageStoreImpl)queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }
        
        boolean snapshotLoaded = false;
        
//        if (!snapshotLoaded) snapshotLoaded = DefaultMessageStoreImpl.loadSnapshot();

        //Step1: 发送消息
        long sendStart = System.currentTimeMillis();
        long maxTimeStamp = System.currentTimeMillis() + sendTime;
        if (!snapshotLoaded) {
	        AtomicLong sendCounter = new AtomicLong(0);
	        Thread[] sends = new Thread[sendTsNum];
	        for (int i = 0; i < sendTsNum; i++) {
	            sends[i] = new Thread(new Producer(messageStore, maxTimeStamp, msgNum, sendCounter));
	        }
	        for (int i = 0; i < sendTsNum; i++) {
	            sends[i].start();
	        }
	        for (int i = 0; i < sendTsNum; i++) {
	            sends[i].join();
	        }
        }
        long sendSend = System.currentTimeMillis();
        System.out.printf("Send: %d ms Num:%d\n", sendSend - sendStart, msgNum);
        long maxCheckTime = System.currentTimeMillis() + checkTime;

        //Step2: 查询聚合消息
        long msgCheckStart = System.currentTimeMillis();
        AtomicLong msgCheckTimes = new AtomicLong(0);
        AtomicLong msgCheckNum = new AtomicLong(0);
        Thread[] msgChecks = new Thread[checkTsNum];
        if (TESTMODE == 1) {
	        for (int i = 0; i < checkTsNum; i++) {
	            msgChecks[i] = new Thread(new MessageChecker(messageStore, maxCheckTime, /*checkTimes*/getMessageTimes, msgNum, maxMsgCheckSize, msgCheckTimes, msgCheckNum));
	        }
	        for (int i = 0; i < checkTsNum; i++) {
	            msgChecks[i].start();
	        }
	        for (int i = 0; i < checkTsNum; i++) {
	            msgChecks[i].join();
	        }
        }
        long msgCheckEnd = System.currentTimeMillis();
        System.out.printf("Message Check: %d ms Num:%d\n", msgCheckEnd - msgCheckStart, msgCheckNum.get());

//        if (!snapshotLoaded) {
//        	DefaultMessageStoreImpl.saveSnapshot();
//        }
        
        
        for (int r = 0; r < 1; r++) {
	        //Step3: 查询聚合结果
	        long checkStart = System.currentTimeMillis();
	        AtomicLong valueCheckTimes = new AtomicLong(0);
	        AtomicLong valueCheckNum = new AtomicLong(0);
	        Thread[] checks = new Thread[checkTsNum];
	        for (int i = 0; i < checkTsNum; i++) {
	            checks[i] = new Thread(new ValueChecker(messageStore, maxCheckTime, checkTimes, msgNum, maxValueCheckSize, valueCheckTimes, valueCheckNum));
	        }
	        for (int i = 0; i < checkTsNum; i++) {
	            checks[i].start();
	        }
	        for (int i = 0; i < checkTsNum; i++) {
	            checks[i].join();
	        }
	        long checkEnd = System.currentTimeMillis();
	        System.out.printf(r + " - Value Check: %d ms Num: %d\n", checkEnd - checkStart, valueCheckNum.get());
	
	        //评测结果
	//        System.out.printf("Total Score:%d\n", (msgNum / (sendSend- sendStart) + msgCheckNum.get() / (msgCheckEnd - msgCheckStart) + valueCheckNum.get() / (checkEnd - checkStart)));
	        System.out.printf(r + " - Value Check Score:%d\n", valueCheckNum.get() / (checkEnd - checkStart));
        }
	        
    }
    static class Producer implements Runnable {

        private AtomicLong counter;
        private long maxMsgNum;
        private MessageStore messageStore;
        private long maxTimeStamp;
        public Producer(MessageStore messageStore, long maxTimeStamp, int maxMsgNum, AtomicLong counter) {
            this.counter = counter;
            this.maxMsgNum = maxMsgNum;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
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
        static final byte[] bodyTemplate = hexStringToByteArray("0000000000158BE00000000000160BE50D2125260B5E5B2B0C3741265C0C36070000");
		
    	private static byte[] getBody(long t, long a)
    	{
    		ByteBuffer buffer = ByteBuffer.allocate(34);
    		buffer.put(bodyTemplate);
    		buffer.putLong(0, t);
    		buffer.putLong(8, a);
    		return buffer.array();
    	}
    	
        @Override
        public void run() {
            long count;
            while (true) {
            	//synchronized(counter) {
	            	if (!((count = counter.getAndIncrement()) < maxMsgNum && System.currentTimeMillis() <= maxTimeStamp)) break;
	                try {
	                    //ByteBuffer buffer = ByteBuffer.allocate(8);
	                    //buffer.putLong(0, count);
	                	
	                    
	                    // 为测试方便, 插入的是有规律的数据, 不是实际测评的情况
	                    messageStore.put(new Message(genAfromT(count), count, getBody(count, genAfromT(count))));
	                    if ((count & 0x1L) == 0) {
	                        //偶数count多加一条消息
	                        messageStore.put(new Message(genAfromT(count), count, getBody(count, genAfromT(count))));
	                    }
	                } catch (Throwable t) {
	                    t.printStackTrace();
	                    System.exit(-1);
	                }
            	//}
            }
        }
    }

    static class MessageChecker implements Runnable {

        private AtomicLong timesCounter;
        private AtomicLong numCounter;
        private long checkTimes;
        private MessageStore messageStore;
        private long maxTimeStamp;
        private int maxIndex;
        private int maxCheckSize;

        public MessageChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize,
                AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError() {
            System.out.println("message check error");
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    int aIndex1 = random.nextInt(maxIndex - 1);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    int aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);

                    int tIndex1 = random.nextInt(aIndex2 - aIndex1) + aIndex1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    int tIndex2 = random.nextInt(maxCheckSize) + tIndex1;

                    List<Message> msgs = messageStore.getMessage(aIndex1, aIndex2, tIndex1, tIndex2);

                    //验证消息
                    Iterator<Message> iter = msgs.iterator();
                    for (int index1 = tIndex1; index1 <= tIndex2 && index1 <= maxIndex; index1++) {
                        

                        if (aIndex1 <= genAfromT(index1) && genAfromT(index1) <= aIndex2) {
                        	if (!iter.hasNext()) {
                        		System.out.println(String.format("ERROR1 t=%d a=%d ; a %d %d; t %d %d; result=%d", index1, genAfromT(index1), aIndex1, aIndex2, tIndex1, tIndex2, msgs.size()));
	                            checkError();
                        	}
	                        Message msg = iter.next();
	                        if (msg.getA() != genAfromT(msg.getT()) || msg.getT() != index1 ||
	                                ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
	                        	System.out.println("T="+ msg.getT());
	                        	System.out.println("A="+ msg.getA());
	                        	System.out.println("index1="+ index1);
	                        	System.out.println("body="+ByteBuffer.wrap(msg.getBody()).getLong());
	                        	System.out.println("ERROR2");
	                            checkError();
	                        }
	
	                        //偶数需要多验证一次
	                        if ((index1 & 0x1) == 0) {
	                        	if (!iter.hasNext()) {
	                        		System.out.println("ERROR3.0");
	                                checkError();
	                        	}
	                            msg = iter.next();
	                            if (msg.getA() != genAfromT(msg.getT()) || msg.getT() != index1
	                                    || ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
	                            	System.out.println("ERROR3");
	                                checkError();
	                            }
	                        }
                        }

                    }


                    if (iter.hasNext()) {
                    	System.out.println("ERROR4");
                        checkError();
                    }

                    numCounter.getAndAdd(msgs.size());
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }
    }

    static class ValueChecker implements Runnable {

        private AtomicLong timesCounter;
        private AtomicLong numCounter;
        private long checkTimes;
        private MessageStore messageStore;
        private long maxTimeStamp;
        private int maxIndex;
        private int maxCheckSize;

        public ValueChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize,
                AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError(long aMin, long aMax, long tMin, long tMax, long res, long val) {
            System.out.printf("value check error. aMin:%d, aMax:%d, tMin:%d, tMax:%d, res:%d, val:%d\n",
                    aMin, aMax, tMin, tMax, res, val);
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    int aIndex1 = random.nextInt(maxIndex - 1);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    int aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);

                    int tIndex1 = random.nextInt(aIndex2 - aIndex1) + aIndex1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    int tIndex2 = random.nextInt(maxCheckSize) + tIndex1;

                    long val = messageStore.getAvgValue(aIndex1, aIndex2, tIndex1, tIndex2);
                    
                    long sum = 0;
                    long count = 0;
                    
                    /*
                    // UNIVERSAL BRUTE FORCE METHOD
                    for (int idx = tIndex1; idx <= tIndex2 && idx <= maxIndex; idx++) {
                    	int nr = idx % 2 == 0 ? 2 : 1;
                    	for (int r = 0; r < nr; r++) {
	                    	if (aIndex1 <= genAfromT(idx) && genAfromT(idx) <= aIndex2) {
	                    		sum += genAfromT(idx);
	                    		count++;
	                    	}
                    	}
                    }
                    //System.out.println("sum=" + sum + " count=" + count);
                    //sum = 0; count = 0;
                    */
                    
                    long tRangeL = tIndex1;
                    long tRangeR = Math.min(tIndex2, maxIndex);
                    
                    long tOddRangeL = tRangeL % 2 == 0 ? tRangeL + 1 : tRangeL;
                    long tOddRangeR = tRangeR % 2 == 0 ? tRangeR - 1 : tRangeR;
                    if (tOddRangeL <= tOddRangeR) {
                    	long aOddRangeL = tOddRangeL - 500;
                    	long aOddRangeR = tOddRangeR - 500;
                    	
                    	aOddRangeL = Math.max(aOddRangeL, aIndex1);
                    	aOddRangeR = Math.min(aOddRangeR, aIndex2);
                    	
                    	if (aOddRangeL % 2 == 0) aOddRangeL++;
                    	if (aOddRangeR % 2 == 0) aOddRangeR--;
                    	
                    	if (aOddRangeL <= aOddRangeR) {
	                    	long aOddCount = (aOddRangeR - aOddRangeL) / 2 + 1;
	                    	count += aOddCount;
	                    	sum += (aOddRangeL + aOddRangeR) * aOddCount / 2;
                    	}
                    }
                    
                    
                    long tEvenRangeL = tRangeL % 2 == 1 ? tRangeL + 1 : tRangeL;
                    long tEvenRangeR = tRangeR % 2 == 1 ? tRangeR - 1 : tRangeR;
                    if (tEvenRangeL <= tEvenRangeR) {
                    	long aEvenRangeL = tEvenRangeL + 30000;
                    	long aEvenRangeR = tEvenRangeR + 30000;
                    	
                    	aEvenRangeL = Math.max(aEvenRangeL, aIndex1);
                    	aEvenRangeR = Math.min(aEvenRangeR, aIndex2);
                    	
                    	if (aEvenRangeL % 2 == 1) aEvenRangeL++;
                    	if (aEvenRangeR % 2 == 1) aEvenRangeR--;
                    	
                    	if (aEvenRangeL <= aEvenRangeR) {
	                    	long aEvenCount = (aEvenRangeR - aEvenRangeL) / 2 + 1;
	                    	count += aEvenCount * 2;
	                    	sum += (aEvenRangeL + aEvenRangeR) * aEvenCount / 2 * 2;
                    	}
                    }
                    
                    //System.out.println("sum=" + sum + " count=" + count);
                    long res = count == 0 ? 0 : sum / count;
                    if (res != val) {
                        checkError(aIndex1, aIndex2, tIndex1, tIndex2, res, val);
                    }

                    numCounter.getAndAdd(count);
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }
    }
}


