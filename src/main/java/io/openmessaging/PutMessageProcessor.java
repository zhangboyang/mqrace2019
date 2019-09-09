package io.openmessaging;

import static io.openmessaging.SliceManager.MAXMSG;
import static io.openmessaging.SliceManager.aSamples;
import static io.openmessaging.SliceManager.globalMaxA;
import static io.openmessaging.SliceManager.globalMinA;
import static io.openmessaging.SliceManager.globalTotalRecords;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.util.ValueCompressor;


public class PutMessageProcessor {

    static final int MAXPUTTHREAD = 100;      // 最大线程数
    
    private static final int SAMPLE_P = MAXMSG / 10000;   // 抽样概率的倒数

    
    static class PutThreadLocalData {
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
    
    static final PutThreadLocalData putTLD[] = new PutThreadLocalData[MAXPUTTHREAD];
    static final AtomicInteger putThreadCount = new AtomicInteger();
    static int nPutThread;
    
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
    
    
    static void flushPutBuffer() throws IOException
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
		
    	// 计算A的范围
		for (int i = 0; i < nThread; i++) {
    		globalMaxA = Math.max(globalMaxA, putTLD[i].maxA);
    		globalMinA = Math.min(globalMinA, putTLD[i].minA);
    	}
		System.out.println(String.format("globalMinA=%d", globalMinA));
		System.out.println(String.format("globalMaxA=%d", globalMaxA));
		System.out.println(String.format("total: %d", globalTotalRecords));
    }
    
    static void doPutMessage(Message message) throws IOException
    {
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
		
    }
    	
}
