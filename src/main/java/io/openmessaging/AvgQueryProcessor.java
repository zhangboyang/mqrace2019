package io.openmessaging;

import static io.openmessaging.FileStorageManager.*;
import static io.openmessaging.IndexMetadata.*;
import static io.openmessaging.SliceManager.*;
import static io.openmessaging.util.Util.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import io.openmessaging.util.ValueCompressor;

public class AvgQueryProcessor {

	private static final int MAXAVGTHREAD = 100;      // 最大线程数
    
    private static int forcePlanId = -1; // 预热/调试时候用
    
    
    private static final AverageResult queryTLD[] = new AverageResult[MAXAVGTHREAD];
    private static final AtomicInteger queryThreadCount = new AtomicInteger();
    private static final ThreadLocal<AverageResult> averageResult = new ThreadLocal<AverageResult>() {
        @Override protected AverageResult initialValue() {
        	return queryTLD[queryThreadCount.getAndIncrement()];
        }
    };
    
    static {
    	for (int i = 0; i < MAXAVGTHREAD; i++) {
    		queryTLD[i] = new AverageResult();
    	}
    }
    
    
    private static final int MAXPLAN = 5;
    private static final double IOSIZE_FACTOR = 27400; // SSD速度为 200MB/s 10000IOPS  这样算每个IO大约20KB
    
    private static class AverageResult {
    	
    	//////////////
    	// 当前查询的a的“和”、“个数”
    	long sum;
    	int cnt;
    	
		//////////////
    	// 当前查询的范围
    	long tMin;
    	long tMax;
    	long aMin;
    	long aMax;
    	
    	//////////////
    	// tMin、tMax、aMin、aMax对应的分片ID号
    	int tSliceLow;
    	int tSliceHigh;
    	int aSliceLow;
    	int aSliceHigh;
    	int aSlice2Low;
    	int aSlice2High;
    	int aSlice3Low;
    	int aSlice3High;
    	
    	//////////////
    	// 当前查询所消耗的IO代价（IO次数、IO字节数）
    	long tAxisIOCount;
    	long tAxisIOBytes;
    	
    	long aAxisIOCount;
    	long aAxisIOBytes;
    	
    	//////////////
    	// 当前查询IO请求命中缓存的次数
    	int nHit;
    	
    	
    	
    	
    	
    	//////////////
    	// 查询计划器相关数据
    	int curPlan; // 当前planID号
    	double ioCost[] = new double[MAXPLAN]; // 各plan的IO代价
    	
    	void addIOCost(long nBytes)
    	{
    		// 若IO字节数太小，则按IO次数为1计算代价
    		// 若IO字节数太大，则把IO字节数换算成IO次数，计算代价
    		ioCost[curPlan] += Math.max(1.0, nBytes / IOSIZE_FACTOR);
    	}
    	
    	
    	
    	//////////////
    	void reset()
    	{
    		sum = 0;
    		cnt = 0;
    		
    		tAxisIOCount = 0;
    		tAxisIOBytes = 0;
    		aAxisIOCount = 0;
    		aAxisIOBytes = 0;
    		
    		nHit = 0;
    	}
    }
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////
    // 算法0：查询矩形的四个边
    
    private static void queryAverageSliceT(AverageResult result, boolean doRealQuery, int tSliceId, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
		int baseOffset = blockOffsetTableAxisT(tSliceId, aSliceLow);
		int nRecord = blockOffsetTableAxisT(tSliceId, aSliceHigh + 1) - baseOffset;
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++;
		result.tAxisIOBytes += nRecord * 16;
		
		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisPointChannel.read(pointBuffer, (long)baseOffset * 16);
		pointBuffer.position(0);
		LongBuffer pointBufferL = pointBuffer.asLongBuffer();
		
		for (int i = 0; i < nRecord; i++) {
			long t = pointBufferL.get();
			long a = pointBufferL.get();
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    
    
    private static void queryAverageSliceA(AverageResult result, boolean doRealQuery, int tSliceLow, int tSliceHigh, int aSliceLow, int aSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	int baseOffsetLow = blockOffsetTableAxisA(tSliceLow, aSliceLow);
		int nRecordLow = blockOffsetTableAxisA(tSliceHigh + 1, aSliceLow) - baseOffsetLow;

		int baseOffsetHigh = blockOffsetTableAxisA(tSliceLow, aSliceHigh);
		int nRecordHigh = blockOffsetTableAxisA(tSliceHigh + 1, aSliceHigh) - baseOffsetHigh;

		if (aSliceLow != aSliceHigh) {
			result.addIOCost((long)nRecordLow * 8);
			result.addIOCost((long)nRecordHigh * 8);
		} else {
			result.addIOCost((long)nRecordLow * 8);
		}
		if (!doRealQuery) return;
		result.aAxisIOCount += 2;
		result.aAxisIOBytes += (nRecordLow + nRecordHigh) * 8;
		
		ByteBuffer lowBuffer = ByteBuffer.allocate(nRecordLow * 8);
		lowBuffer.order(ByteOrder.LITTLE_ENDIAN);
		aAxisIndexChannel.read(lowBuffer, (long)baseOffsetLow * 8);
		lowBuffer.position(0);
		LongBuffer lowBufferL = lowBuffer.asLongBuffer();
		
		ByteBuffer highBuffer = ByteBuffer.allocate(nRecordHigh * 8);
		highBuffer.order(ByteOrder.LITTLE_ENDIAN);
		if (aSliceLow != aSliceHigh) {
			aAxisIndexChannel.read(highBuffer, (long)baseOffsetHigh * 8);
		} else {
			highBuffer.put(lowBuffer);
		}
		highBuffer.position(0);
		LongBuffer highBufferL = highBuffer.asLongBuffer();
		
		int lowOffset = 0;
		int highOffset = 0;
		for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
			
			int lowCount = blockOffsetTableAxisA(tSliceId + 1, aSliceLow) - blockOffsetTableAxisA(tSliceId, aSliceLow) - 1;
			int highCount = blockOffsetTableAxisA(tSliceId + 1, aSliceHigh) - blockOffsetTableAxisA(tSliceId, aSliceHigh) - 1;
			
			long lastPrefixSum = lowBufferL.get(lowOffset + lowCount);
			long lowSum = lastPrefixSum;
			int lowPtr = -1;
			for (int i = lowOffset; i < lowOffset + lowCount; i++) {
				long prefixSum = lowBufferL.get(i);
				long a = prefixSum - lastPrefixSum;
				lastPrefixSum = prefixSum;
				if (a < aMin) {
					lowSum = prefixSum;
					lowPtr = i - lowOffset;
				} else {
					break;
				}
			}
			lowPtr++;
			
			lastPrefixSum = highBufferL.get(highOffset + highCount);
			long highSum = lastPrefixSum;
			int highPtr = -1;
			for (int i = highOffset; i < highOffset + highCount; i++) {
				long prefixSum = highBufferL.get(i);
				long a = prefixSum - lastPrefixSum;
				lastPrefixSum = prefixSum;
				if (a <= aMax) {
					highSum = highBufferL.get(i);
					highPtr = i - highOffset;
				}
			}
			
			
			int globalLowPtr = blockOffsetTableAxisT(tSliceId, aSliceLow) + lowPtr;
			int globalHighPtr = blockOffsetTableAxisT(tSliceId, aSliceHigh) + highPtr;
			
			long sum = 0;
			int cnt = 0;
			if (globalHighPtr >= globalLowPtr) {
				sum = highSum - lowSum;
				cnt = globalHighPtr - globalLowPtr + 1;
			}
			
			result.sum += sum;
			result.cnt += cnt;
		
//			AverageResult referenceResult = new AverageResult();
//			queryAverageSliceT(referenceResult, doRealQuery, tSliceId, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
//			System.out.println(String.format("tSliceId=%d ; aSliceLow=%d aSliceHigh=%d ; lowPtr=%d  highPtr=%d", tSliceId, aSliceLow, aSliceHigh, lowPtr, highPtr));
//			System.out.println(String.format("cnt=%d sum=%d", cnt, sum));
//			System.out.println(String.format("ref: cnt=%d sum=%d", referenceResult.cnt, referenceResult.sum));
//			assert cnt == referenceResult.cnt;
//			assert sum == referenceResult.sum;

			
			lowOffset += lowCount + 1;
			highOffset += highCount + 1;
		}
		
		assert lowOffset == nRecordLow;
		assert highOffset == nRecordHigh;
    }
    
    private static void queryAlgorithm0(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSliceLow = result.aSliceLow;
    	int aSliceHigh = result.aSliceHigh;
    	
    	if (tSliceLow == tSliceHigh) {
    		// 在同一个a块内，只能暴力
    		queryAverageSliceT(result, doRealQuery, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		
    	} else {
    		
    		queryAverageSliceT(result, doRealQuery, tSliceLow, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		queryAverageSliceT(result, doRealQuery, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		tSliceLow++;
    		tSliceHigh--;
    		if (tSliceLow <= tSliceHigh) {
    			queryAverageSliceA(result, doRealQuery, tSliceLow, tSliceHigh, aSliceLow, aSliceHigh, tMin, tMax, aMin, aMax);
    		}
    	}
    }
    
    
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法1：对t轴上的分块进行暴力查找
    
    private static void queryAlgorithm1(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSliceLow = result.aSliceLow;
    	int aSliceHigh = result.aSliceHigh;
    	
		int baseOffset = blockOffsetTableAxisT(tSliceLow, aSliceLow);
		int nRecord = blockOffsetTableAxisT(tSliceHigh, aSliceHigh + 1) - baseOffset;
		
		
		result.addIOCost((long)nRecord * 16);
		if (!doRealQuery) return;
		result.tAxisIOCount++; // FIXME: 分开统计？
		result.tAxisIOBytes += nRecord * 16;
		
		ByteBuffer pointBuffer = ByteBuffer.allocate(nRecord * 16);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisPointChannel.read(pointBuffer, (long)baseOffset * 16);
		pointBuffer.position(0);
		LongBuffer pointBufferL = pointBuffer.asLongBuffer();
		
		for (int i = 0; i < nRecord; i++) {
			long t = pointBufferL.get();
			long a = pointBufferL.get();
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    
    
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法2：对t轴上的压缩分块进行暴力查找
    
    private static void queryAlgorithm2(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	
		long baseOffset = tSliceCompressedPointByteOffset[tSliceLow];
		long nBytes = tSliceCompressedPointByteOffset[tSliceHigh + 1] - baseOffset;
		if (nBytes != (int)nBytes || !CachedCompressedPointAxisT.hit(baseOffset, (int)nBytes)) {
			result.addIOCost(nBytes);
			if (!doRealQuery) return;
			result.tAxisIOCount++; // FIXME: 分开统计？
			result.tAxisIOBytes += nBytes;
		} else {
			if (!doRealQuery) return;
			result.nHit++;
		}
		
		
		
		
		ByteBuffer pointBuffer = ByteBuffer.allocate((int)nBytes);
		pointBuffer.order(ByteOrder.LITTLE_ENDIAN);
//		tAxisCompressedPointChannel.read(pointBuffer, baseOffset);
		CachedCompressedPointAxisT.read(pointBuffer, baseOffset);
		pointBuffer.position(0);
		
		for (int tSliceId = tSliceLow; tSliceId <= tSliceHigh; tSliceId++) {
			int nRecord = tSliceRecordCount[tSliceId];
			long t = tSlicePivot[tSliceId];
			for (int i = 0; i < nRecord; i++) {
				t += ValueCompressor.getFromBuffer(pointBuffer);
				long a = ValueCompressor.getFromBuffer(pointBuffer);
//				System.out.println("t=" + t + " a=" + a);
		
				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
					result.sum += a;
					result.cnt++;
				}
			}
		}
		assert pointBuffer.position() == pointBuffer.capacity();
    }
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法3：对a轴上的压缩分块进行暴力查找
    
    private static void queryAZP2(AverageResult result, boolean doRealQuery, int aSlice2Id, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	long nBytes = aAxisCompressedPoint2ByteOffset.get(tSliceHigh + 1, aSlice2Id) - aAxisCompressedPoint2ByteOffset.get(tSliceLow, aSlice2Id);
		
    	result.addIOCost(nBytes);
		if (!doRealQuery) return;
		result.aAxisIOCount++; // FIXME: 分开统计？
		result.aAxisIOBytes += nBytes;
		
    	ByteBuffer buffer = ByteBuffer.allocate((int)nBytes).order(ByteOrder.LITTLE_ENDIAN);
    	aAxisCompressedPoint2Channel[aSlice2Id].read(buffer, aAxisCompressedPoint2ByteOffset.get(tSliceLow, aSlice2Id));
    	buffer.position(0);
    	
    	
    	long t = aAxisCompressedPoint2BaseT.get(tSliceLow, aSlice2Id);
		while (buffer.hasRemaining()) {
			t += ValueCompressor.getFromBuffer(buffer);
			long a = ValueCompressor.getFromBuffer(buffer);
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    private static void queryAlgorithm3(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSlice2Low = result.aSlice2Low;
    	int aSlice2High = result.aSlice2High;
    	
    	for (int aSlice2Id = aSlice2Low; aSlice2Id <= aSlice2High; aSlice2Id++) {
    		queryAZP2(result, doRealQuery, aSlice2Id, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    	}
    }
    
    
    
	////////////////////////////////////////////////////////////////////////////////////////
	// 算法4：对a轴上的3号压缩分块进行暴力查找
    
    private static void queryAZP3(AverageResult result, boolean doRealQuery, int aSlice3Id, int tSliceLow, int tSliceHigh, long tMin, long tMax, long aMin, long aMax) throws IOException
    {
    	long nBytes = aAxisCompressedPoint3ByteOffset.get(tSliceHigh + 1, aSlice3Id) - aAxisCompressedPoint3ByteOffset.get(tSliceLow, aSlice3Id);
		
    	result.addIOCost(nBytes);
		if (!doRealQuery) return;
		result.aAxisIOCount++; // FIXME: 分开统计？
		result.aAxisIOBytes += nBytes;
		
    	ByteBuffer buffer = ByteBuffer.allocate((int)nBytes).order(ByteOrder.LITTLE_ENDIAN);
    	aAxisCompressedPoint3Channel[aSlice3Id].read(buffer, aAxisCompressedPoint3ByteOffset.get(tSliceLow, aSlice3Id));
    	buffer.position(0);
    	
    	long t = aAxisCompressedPoint3BaseT.get(tSliceLow, aSlice3Id);
		while (buffer.hasRemaining()) {
			t += ValueCompressor.getFromBuffer(buffer);
			long a = ValueCompressor.getFromBuffer(buffer);
			
			if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
				result.sum += a;
				result.cnt++;
			}
		}
    }
    private static void queryAlgorithm4(AverageResult result, boolean doRealQuery) throws IOException
    {
    	long tMin = result.tMin;
    	long tMax = result.tMax;
    	long aMin = result.aMin;
    	long aMax = result.aMax;
    	int tSliceLow = result.tSliceLow;
    	int tSliceHigh = result.tSliceHigh;
    	int aSlice3Low = result.aSlice3Low;
    	int aSlice3High = result.aSlice3High;
    	
    	for (int aSlice3Id = aSlice3Low; aSlice3Id <= aSlice3High; aSlice3Id++) {
    		queryAZP3(result, doRealQuery, aSlice3Id, tSliceLow, tSliceHigh, tMin, tMax, aMin, aMax);
    	}
    }
    
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////

    ///// 查询执行器：给定算法Id号，执行对应的查询
    private static void queryExecutor(AverageResult result, int planId, boolean doRealQuery) throws IOException
    {
    	result.curPlan = planId;
    	result.ioCost[planId] = 0;
    	
    	switch (planId) {
    	case 0: queryAlgorithm0(result, doRealQuery); break;
    	case 1: queryAlgorithm1(result, doRealQuery); break;
    	case 2: queryAlgorithm2(result, doRealQuery); break;
    	case 3: queryAlgorithm3(result, doRealQuery); break;
    	case 4: queryAlgorithm4(result, doRealQuery); break;
    	default: assert false;
    	}
    }
    
    
    ////// 查询计划器：预估不同查询算法IO代价，选择IO代价最小的算法Id号返回
    private static int queryPlanner(AverageResult result, boolean doRealQuery) throws IOException
    {
    	if (forcePlanId >= 0) {
    		return forcePlanId;
    	}
    	
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		queryExecutor(result, planId, false);
    	}
    	
    	double minIOCost = 1e100;
    	int optimalPlanId = -1;
    	for (int planId = 0; planId < MAXPLAN; planId++) {
    		if (result.ioCost[planId] < minIOCost) {
    			minIOCost = result.ioCost[planId];
    			optimalPlanId = planId;
    		}
    	}
    	
//    	optimalPlanId = 2;
    	return optimalPlanId;
    }
    

    
    ////////////////////////////////////////////////////////////////////////////////////////

    
    private static final AtomicInteger totalAvgQuery = new AtomicInteger();
    private static final AtomicLong totalAvgRecords = new AtomicLong();
	private static final AtomicLong tAxisIOCountTotal = new AtomicLong();
	private static final AtomicLong tAxisIOBytesTotal = new AtomicLong();
	private static final AtomicLong aAxisIOCountTotal = new AtomicLong();
	private static final AtomicLong aAxisIOBytesTotal = new AtomicLong();
	private static final DoubleAdder totalIOCost = new DoubleAdder();
	private static final AtomicIntegerArray planCount = new AtomicIntegerArray(MAXPLAN);
	private static final AtomicInteger totalCacheHit = new AtomicInteger();

    static long doGetAvgValue(long aMin, long aMax, long tMin, long tMax) throws IOException
    {

    	AverageResult result = averageResult.get();
    	
    	result.reset();
    	
    	result.tMin = tMin;
    	result.tMax = tMax;
    	result.aMin = aMin;
    	result.aMax = aMax;
    	
    	result.tSliceLow = findSliceT(tMin);
    	result.tSliceHigh = findSliceT(tMax);
    	result.aSliceLow = findSliceA(aMin);
    	result.aSliceHigh = findSliceA(aMax);
    	result.aSlice2Low = findSliceA2(aMin);
    	result.aSlice2High = findSliceA2(aMax);
    	result.aSlice3Low = findSliceA3(aMin);
    	result.aSlice3High = findSliceA3(aMax);

		// 不同查询算法的IO代价可能不同
		// 这里模仿数据库的查询计划器，先预估每种算法的IO代价，挑选最小的那个去执行
		int optimalPlanId = queryPlanner(result, false);
		queryExecutor(result, optimalPlanId, true);

    	
    	totalAvgQuery.incrementAndGet();
    	totalAvgRecords.addAndGet(result.cnt);
    	tAxisIOCountTotal.addAndGet(result.tAxisIOCount);
    	tAxisIOBytesTotal.addAndGet(result.tAxisIOBytes);
    	aAxisIOCountTotal.addAndGet(result.aAxisIOCount);
    	aAxisIOBytesTotal.addAndGet(result.aAxisIOBytes);
    	planCount.incrementAndGet(result.curPlan);
    	totalIOCost.add(result.ioCost[result.curPlan]);
    	totalCacheHit.addAndGet(result.nHit);
    	
    	return result.cnt == 0 ? 0 : result.sum / result.cnt;
    }
	
	private static void resetAvgQueryStatistics()
	{
		queryThreadCount.set(0);
		
	    totalAvgQuery.set(0);
	    totalAvgRecords.set(0);
		tAxisIOCountTotal.set(0);
		tAxisIOBytesTotal.set(0);
		aAxisIOCountTotal.set(0);
		aAxisIOBytesTotal.set(0);
		totalIOCost.reset();
		for (int i = 0; i < MAXPLAN; i++) {
			planCount.set(i, 0);
		}
	}
	
    static void printAvgQueryStatistics()
    {
    	System.out.println("[" + new Date() + "]: shutdown hook");
    	
    	
    	System.out.println(String.format("totalAvgRecords=%d", totalAvgRecords.get()));
    	System.out.println(String.format("totalAvgQuery=%d", totalAvgQuery.get()));
    	System.out.println(String.format("tAxisIOCountTotal=%d", tAxisIOCountTotal.get()));
    	System.out.println(String.format("tAxisIOBytesTotal=%d", tAxisIOBytesTotal.get()));
    	System.out.println(String.format("aAxisIOCountTotal=%d", aAxisIOCountTotal.get()));
    	System.out.println(String.format("aAxisIOBytesTotal=%d", aAxisIOBytesTotal.get()));
    	System.out.println(String.format("totalCacheHit=%d", totalCacheHit.get()));
    	for (int i = 0; i < MAXPLAN; i++) {
    		System.out.println(String.format("planCount[%d]=%d", i, planCount.get(i)));
    	}
    	System.out.println(String.format("totalIOCost=%f", totalIOCost.sum()));
    	System.out.println(String.format("IO per Query: %f", totalIOCost.sum() / totalAvgQuery.get()));
    }
}
