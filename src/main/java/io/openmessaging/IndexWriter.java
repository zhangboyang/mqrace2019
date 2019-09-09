package io.openmessaging;

import static io.openmessaging.FileStorageManager.*;
import static io.openmessaging.IndexMetadata.*;
import static io.openmessaging.IndexWriter.buildIndex;
import static io.openmessaging.IndexWriter.externalMergeSort;
import static io.openmessaging.SliceManager.*;
import static io.openmessaging.util.Util.*;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Date;

import io.openmessaging.util.ValueCompressor;
import static io.openmessaging.PutMessageProcessor.*;

public class IndexWriter {

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  建立a方向上的前缀和索引
    //
    
    private static ByteBuffer indexReadBuffer = null;
    private static void reserveIndexReadBuffer(int nBytes)
    {
        if (indexReadBuffer == null || indexReadBuffer.capacity() < nBytes) {
            indexReadBuffer = ByteBuffer.allocate(nextSize(nBytes));
            indexReadBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }
    private static ByteBuffer indexWriteBuffer = null;
    private static void reserveIndexWriteBuffer(int nBytes)
    {
        if (indexWriteBuffer == null || indexWriteBuffer.capacity() < nBytes) {
            indexWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
            indexWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }
    
    private static final int BATCHSIZE = 5000; // 一次读进内存的t分片数量

    private static void buildIndexForRangeAxisA(int tSliceFrom, int tSliceTo) throws IOException
    {
//      System.out.println("[" + new Date() + "]: " + String.format("from=%d to=%d", tSliceFrom, tSliceTo));
        
        int nRecord = tSliceRecordOffset[tSliceTo + 1] - tSliceRecordOffset[tSliceFrom];
        reserveIndexReadBuffer(nRecord * 16);
        reserveIndexWriteBuffer((nRecord + BATCHSIZE * N_ASLICE) * 8);
        
        assert tAxisPointData.getFilePointer() == (long)tSliceRecordOffset[tSliceFrom] * 16;
        tAxisPointData.readFully(indexReadBuffer.array(), 0, nRecord * 16);
        indexReadBuffer.position(0);
        LongBuffer indexReadBufferL = indexReadBuffer.asLongBuffer();
        
        indexWriteBuffer.position(0);
        LongBuffer indexWriteBufferL = indexWriteBuffer.asLongBuffer();

        
        // 造a轴前缀和索引
        int sliceRecordCount[] = new int[N_ASLICE];
        int bufferBase[] = new int[N_ASLICE];
        for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
            sliceRecordCount[aSliceId] = blockOffsetTableAxisA(tSliceTo + 1, aSliceId) - blockOffsetTableAxisA(tSliceFrom, aSliceId);
            if (aSliceId > 0) {
                bufferBase[aSliceId] = bufferBase[aSliceId - 1] + sliceRecordCount[aSliceId - 1];
            }
        }
        
        int msgPtr = 0;
        for (int tSliceId = tSliceFrom; tSliceId <= tSliceTo; tSliceId++) {
            
            long prefixSum = 0;
            
            for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
                int msgCnt = blockOffsetTableAxisA(tSliceId + 1, aSliceId) - blockOffsetTableAxisA(tSliceId, aSliceId) - 1;
                
                int putBase = bufferBase[aSliceId] + blockOffsetTableAxisA(tSliceId, aSliceId) - blockOffsetTableAxisA(tSliceFrom, aSliceId);

                // 存储prefixSumBase
                indexWriteBufferL.put(putBase + msgCnt, prefixSum);
                
                // 存储各个prefixSum
                for (int i = putBase; i < putBase + msgCnt; i++) {
                    
                    long curA = indexReadBufferL.get((msgPtr++ * 2) + 1);
                    prefixSum += curA;
                    
                    indexWriteBufferL.put(i, prefixSum);
                }
            }
        }
        assert msgPtr == nRecord;
        
        for (int aSliceId = 0; aSliceId < N_ASLICE; aSliceId++) {
            aAxisIndexData.seek((long)blockOffsetTableAxisA(tSliceFrom, aSliceId) * 8);
            aAxisIndexData.write(indexWriteBuffer.array(), bufferBase[aSliceId] * 8, sliceRecordCount[aSliceId] * 8);
        }
    }
    
    private static void buildIndexAxisA() throws IOException
    {
        System.out.println("[" + new Date() + "]: build index for a-axis");
        
        tAxisPointData.seek(0);
        reserveDiskSpace(aAxisIndexFile, (long)insCount * 8);
        
        for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId += BATCHSIZE) {
            buildIndexForRangeAxisA(tSliceId, Math.min(tSliceId + BATCHSIZE, tSliceCount) - 1);
        }
        
        System.out.println("[" + new Date() + "]: a-axis index finished");
        
        indexReadBuffer = null;
        indexWriteBuffer = null;
    }
    
    
    
    
    
    
    
    
    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //   构建各式各样的索引（t轴点索引、t轴压缩点索引、a轴2号压缩点索引、a轴3号压缩点索引）
    //   由合并排序来调用
    //
    
    private static class MessageWithMetadata extends Message {
        int threadId;
        int nextRecordId;
        long nextRecordByteOffset;
        
        int aSlice2Id;
        int aSlice3Id;
        
        public MessageWithMetadata(long a, long t, byte[] body) {
            super(a, t, body);
        }
    }
    
    private static MessageWithMetadata writeBuffer[] = null;
    private static MessageWithMetadata writeBuffer2[] = null;
    private static int writeBufferPtr = 0;
    
    // 预留内存的函数：为了避免重复new，数组内的对象会被重用
    private static void reserveWriteBuffer(int n)
    {
        if (writeBuffer == null || writeBuffer.length < n) {
            int oldSize;
            MessageWithMetadata newBuffer[] = new MessageWithMetadata[nextSize(n)];
            if (writeBuffer == null) {
                oldSize = 0;
            } else {
                oldSize = writeBuffer.length;
                System.arraycopy(writeBuffer, 0, newBuffer, 0, oldSize);
            }
            for (int i = oldSize; i < newBuffer.length; i++) {
                newBuffer[i] = new MessageWithMetadata(0, 0, null);
            }
            writeBuffer = newBuffer;
            writeBuffer2 = new MessageWithMetadata[newBuffer.length];
        }
    }
    private static void shiftWriteBuffer(int n)
    {
        // 逻辑上移除前n个数据元素，实际上是将前n个元素循环移至最后
        
        if (writeBufferPtr != n) {
            int m = writeBufferPtr - n;
            
            System.arraycopy(writeBuffer, n, writeBuffer2, 0, m);
            System.arraycopy(writeBuffer, 0, writeBuffer2, m, n);
            System.arraycopy(writeBuffer2, 0, writeBuffer, 0, writeBufferPtr);
        }
        writeBufferPtr -= n;
    }

    
    
    
    
    // 用来存(t,a)数据的buffer
    private static ByteBuffer pointWriteBuffer = null;
    private static void reservePointBuffer(int nBytes)
    {
        if (pointWriteBuffer == null || pointWriteBuffer.capacity() < nBytes) {
            pointWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
            pointWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }
    
    
    
    
    // 用来存body指针的buffer
    private static ByteBuffer bodyidxWriteBuffer = null;
    private static void reserveBodyBuffer(int nBytes)
    {
        if (bodyidxWriteBuffer == null || bodyidxWriteBuffer.capacity() < nBytes) {
            bodyidxWriteBuffer = ByteBuffer.allocate(nextSize(nBytes));
            bodyidxWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }
    
    private static final long zpByteOffset[] = new long[MAXPUTTHREAD];
    private static final int zpRecordOffset[] = new int[MAXPUTTHREAD];
    private static final long zpLastT[] = new long[MAXPUTTHREAD];

    private static void writeBodyData() throws IOException
    {       
        int nThread = nPutThread;
        
        // 写线程t块偏移（供getMessage用）
        reserveBodyBuffer(nThread * 20);
        bodyidxWriteBuffer.position(0);
        for (int i = 0; i < nThread; i++) {
            bodyidxWriteBuffer.putInt(zpRecordOffset[i]);
            bodyidxWriteBuffer.putLong(zpByteOffset[i]);
            bodyidxWriteBuffer.putLong(zpLastT[i]);
        }
        tAxisBodyStream.write(bodyidxWriteBuffer.array(), 0, nThread * 20);
    }

    // 当前一批数据为同一个t分片中的数据，进行批量处理
    private static void flushBatch(long exclusiveT) throws IOException
    {
//      System.out.println(String.format("flush=%d size=%d", tSliceCount - 1, writeBuffer.size()));
        int nWrite;
        for (nWrite = 0; nWrite < writeBufferPtr; nWrite++) {
            MessageWithMetadata curMessage = writeBuffer[nWrite];
            if (curMessage.getT() == exclusiveT) { // 排除要排除的消息
                break;
            }
        }

        int tSliceId = tSliceCount - 1;
        
        tSliceRecordCount[tSliceId] = nWrite;


        for (int i = 0; i < nWrite; i++) {
            MessageWithMetadata msg = writeBuffer[i];
            int threadId = msg.threadId;
            zpRecordOffset[threadId] = msg.nextRecordId;
            zpByteOffset[threadId] = msg.nextRecordByteOffset;
            zpLastT[threadId] = msg.getT();
        }
        writeBodyData();

        // 写t轴压缩索引（为了保证t的压缩效率，t块内部应按照t排序
        reserveBodyBuffer(nWrite * 18);
        ByteBuffer compressedPointBuffer = bodyidxWriteBuffer; // 重用空间，懒得再分开了
        compressedPointBuffer.position(0);
        
        long lastT = tSlicePivot[tSliceId];
        for (int i = 0; i < nWrite; i++) {
            MessageWithMetadata msg = writeBuffer[i];
            long a = msg.getA();
            long t = msg.getT();
            
            long deltaT = t - lastT;
            ValueCompressor.putToBuffer(compressedPointBuffer, deltaT);
            ValueCompressor.putToBuffer(compressedPointBuffer, a);
            lastT = t;
        }
        
        
        // 计算每小块内记录数量，并写每小格的数据，此时t块内部必须按a排序
        reservePointBuffer(nWrite * 16);
        pointWriteBuffer.position(0);
        System.arraycopy(writeBuffer, 0, writeBuffer2, 0, nWrite); // 记录按t排序的顺序到writeBuffer2
        Arrays.sort(writeBuffer, 0, nWrite, aComparator);
        int aSliceId = 0;
        
        int aSlice2Id = 0, aSlice3Id = 0; 
        for (int i = 0; i < N_ASLICE2; i++) {
            aAxisCompressedPoint2BaseT.set(tSliceId + 1, i, aAxisCompressedPoint2BaseT.get(tSliceId, i));
        }
        for (int i = 0; i < N_ASLICE3; i++) {
            aAxisCompressedPoint3BaseT.set(tSliceId + 1, i, aAxisCompressedPoint3BaseT.get(tSliceId, i));
        }
        
        for (int i = 0; i < nWrite; i++) {
            MessageWithMetadata msg = writeBuffer[i];
            long a = msg.getA();
            long t = msg.getT();
            
            while (aSliceId < N_ASLICE && a >= aSlicePivot[aSliceId + 1]) aSliceId++;
            
            incBlockOffsetTableCountPrefixSum(tSliceId, aSliceId);
            
            pointWriteBuffer.putLong(t).putLong(a);
            
            while (aSlice2Id < N_ASLICE2 && a >= aSlice2Pivot[aSlice2Id + 1]) aSlice2Id++;
            msg.aSlice2Id = aSlice2Id;
            while (aSlice3Id < N_ASLICE3 && a >= aSlice3Pivot[aSlice3Id + 1]) aSlice3Id++;
            msg.aSlice3Id = aSlice3Id;

        }
        
        
        ByteBuffer aAxisWriteBuffer = ByteBuffer.allocate(18).order(ByteOrder.LITTLE_ENDIAN);
        
        // 写a轴压缩索引
        for (int i = 0; i < nWrite; i++) {
            MessageWithMetadata msg = writeBuffer2[i];
            long a = msg.getA();
            long t = msg.getT();
            
            aSlice2Id = msg.aSlice2Id;
            long deltaT2 = t - aAxisCompressedPoint2BaseT.get(tSliceId + 1, aSlice2Id);
            
            aAxisWriteBuffer.position(0);
            ValueCompressor.putToBuffer(aAxisWriteBuffer, deltaT2);
            ValueCompressor.putToBuffer(aAxisWriteBuffer, a);
            aAxisCompressedPoint2Data[aSlice2Id].write(aAxisWriteBuffer.array(), 0, aAxisWriteBuffer.position());
            aAxisCompressedPoint2ByteOffset.add(tSliceId, aSlice2Id, aAxisWriteBuffer.position());
            aAxisCompressedPoint2OutputBytes += aAxisWriteBuffer.position();
            aAxisCompressedPoint2BaseT.set(tSliceId + 1, aSlice2Id, t);
        }
        for (int i = 0; i < nWrite; i++) {
            MessageWithMetadata msg = writeBuffer2[i];
            long a = msg.getA();
            long t = msg.getT();
            
            aSlice3Id = msg.aSlice3Id;
            long deltaT3 = t - aAxisCompressedPoint3BaseT.get(tSliceId + 1, aSlice3Id);
            
            aAxisWriteBuffer.position(0);
            ValueCompressor.putToBuffer(aAxisWriteBuffer, deltaT3);
            ValueCompressor.putToBuffer(aAxisWriteBuffer, a);
            aAxisCompressedPoint3Data[aSlice3Id].write(aAxisWriteBuffer.array(), 0, aAxisWriteBuffer.position());
            aAxisCompressedPoint3ByteOffset.add(tSliceId, aSlice3Id, aAxisWriteBuffer.position());
            aAxisCompressedPoint3OutputBytes += aAxisWriteBuffer.position();
            aAxisCompressedPoint3BaseT.set(tSliceId + 1, aSlice3Id, t);
        }
        
        shiftWriteBuffer(nWrite); // 将排除掉的消息移至最前
        
        tAxisPointStream.write(pointWriteBuffer.array(), 0, nWrite * 16);

        tAxisCompressedPointStream.write(compressedPointBuffer.array(), 0, compressedPointBuffer.position());
        tSliceCompressedPointByteOffset[tSliceId + 1] = tSliceCompressedPointByteOffset[tSliceId] + compressedPointBuffer.position();
    }

    private static BufferedOutputStream tAxisPointStream;
    private static BufferedOutputStream tAxisBodyStream;
    private static BufferedOutputStream tAxisCompressedPointStream;
    
    // 插入消息初始化工作
    private static void beginInsertMessage() throws IOException
    {
        reserveDiskSpace(tAxisPointFile, (long)globalTotalRecords * 16);
        reserveDiskSpace(tAxisCompressedPointFile, (long)globalTotalRecords * 10);
        tAxisPointStream = new BufferedOutputStream(new FileOutputStream(tAxisPointFile));
        tAxisBodyStream = new BufferedOutputStream(new FileOutputStream(tAxisBodyFile));
        tAxisCompressedPointStream = new BufferedOutputStream(new FileOutputStream(tAxisCompressedPointFile));
        
        writeBodyData();
    }
    
    private static int insCount = 0; // 当前已插入的消息数
    
    // 插入一条消息
    private static void insertMessage(long curT, long curA, int threadId, int nextRecordId, long nextRecordByteOffset) throws IOException
    {
        // 若当前插入消息数达到分片数量，则把当前这一批数据批量构建索引
        if (insCount % TSLICE_INTERVAL == 0) {
            if (insCount > 0) {
                flushBatch(curT); // 要严格排除t=curT的消息，否则会出现错误
            }
            tSlicePivot[tSliceCount++] = curT; // 新分割点设置为当前消息的t值
        }
        
        // 放入buffer中，待后续批量处理
        reserveWriteBuffer(writeBufferPtr + 1);
        MessageWithMetadata message = writeBuffer[writeBufferPtr++];
        message.setT(curT);
        message.setA(curA);
        message.threadId = threadId;
        message.nextRecordId = nextRecordId;
        message.nextRecordByteOffset = nextRecordByteOffset;
        
        insCount++;
        if (insCount % 1000000 == 0) {
            System.out.println("[" + new Date() + "]: " + String.format("ins %d: %s", insCount, dumpMessage(message)));
        }
    }
    
    // 结束插入消息：进行一些索引的收尾工作
    private static void finishInsertMessage() throws IOException
    {
        flushBatch(Long.MAX_VALUE);
        tSlicePivot[tSliceCount] = Long.MAX_VALUE;
        assert writeBufferPtr == 0;
        
        System.out.println("tSliceCount=" + tSliceCount);
        System.out.println(String.format("tSliceCompressedPointBytes=%d  (%f b/rec)", tSliceCompressedPointByteOffset[tSliceCount], (double)tSliceCompressedPointByteOffset[tSliceCount]/insCount));
        System.out.println(String.format("aAxisCompressedPoint2OutputBytes=%d  (%f b/rec)", aAxisCompressedPoint2OutputBytes, (double)aAxisCompressedPoint2OutputBytes / insCount));
        System.out.println(String.format("aAxisCompressedPoint3OutputBytes=%d  (%f b/rec)", aAxisCompressedPoint3OutputBytes, (double)aAxisCompressedPoint3OutputBytes / insCount));
    
        tAxisPointStream.close();
        tAxisPointStream = null;
        tAxisBodyStream.close();
        tAxisBodyStream = null;
        tAxisCompressedPointStream.close();
        tAxisCompressedPointStream = null;
        
        for (int i = 0; i < N_ASLICE2; i++) {
            aAxisCompressedPoint2Data[i].close();
            aAxisCompressedPoint2Data[i] = null;
        }
        for (int i = 0; i < N_ASLICE3; i++) {
            aAxisCompressedPoint3Data[i].close();
            aAxisCompressedPoint3Data[i] = null;
        }
    }
    

    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    // 外部排序：按t进行精确全局排序
    //    采用经典的多路合并排序算法进行外部排序
    //    排序过程中，会调用insertMessage进行索引构建（边排序边进行索引）
    
    static void externalMergeSort() throws IOException
    {
        System.out.println("[" + new Date().toString() + "]: merge-sort begin!");

        int nThread = nPutThread;
        
        ByteBuffer queueData[] = new ByteBuffer[nThread];
        for (int i = 0; i < nThread; i++) {
            queueData[i] = ByteBuffer.allocate(4096);
            queueData[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        
        int readCount[] = new int[nThread]; 
        int recordCount[] = new int[nThread];
        int bufferCap[] = new int[nThread];
        long readBytes[] = new long[nThread];
        long queueHead[] = new long[nThread];
        
        // 初始化各个队列
        for (int i = 0; i < nThread; i++) {
            recordCount[i] = putTLD[i].outputCount;
            readCount[i] = 0;
            
            if (recordCount[i] > 0) {
                putTLD[i].pointInputStream = new FileInputStream(putTLD[i].pointFileName);
                readBytes[i] = bufferCap[i] = putTLD[i].pointInputStream.read(queueData[i].array(), 0, queueData[i].capacity());
                queueHead[i] = ValueCompressor.getFromBuffer(queueData[i]);
            } else {
                queueHead[i] = Long.MAX_VALUE;
            }
        }
        
        beginInsertMessage();
        
        while (true) {
            
            // 从所有队列中取最小
            long minValue = queueHead[0];
            int minPos = 0;
            for (int i = 1; i < nThread; i++) {
                long curValue = queueHead[i];
                if (curValue < minValue) {
                    minValue = curValue;
                    minPos = i;
                }
            }
            
            // 如果取出的值是哨兵值，说明已经排序完毕，退出循环
            if (minValue == Long.MAX_VALUE) {
                break;
            }
            
            // 插入此条消息
            long aValue = ValueCompressor.getFromBuffer(queueData[minPos]); 
            insertMessage(minValue, aValue, minPos, readCount[minPos] + 1, readBytes[minPos] - bufferCap[minPos] + queueData[minPos].position());
            
            // 向队列中填充数据
            if (++readCount[minPos] >= recordCount[minPos]) {
                queueHead[minPos] = Long.MAX_VALUE;
            } else {
                ByteBuffer buffer = queueData[minPos];
                if (buffer.remaining() < 64) {
                    int nCopy = bufferCap[minPos] - buffer.position();
                    System.arraycopy(buffer.array(), buffer.position(), buffer.array(), 0, nCopy);
                    buffer.position(0);
                    int nReadBytes = (int)Math.min(buffer.capacity() - nCopy, putTLD[minPos].outputBytes - readBytes[minPos]);
                    if (nReadBytes > 0) {
                        putTLD[minPos].pointInputStream.read(buffer.array(), nCopy, nReadBytes);
                        readBytes[minPos] += nReadBytes;
                    }
                    bufferCap[minPos] = nCopy + nReadBytes;
                }
                
                queueHead[minPos] += ValueCompressor.getFromBuffer(buffer);
            }
        }
        
        finishInsertMessage();
        
        for (int i = 0; i < nThread; i++) {
            putTLD[i].pointInputStream.close();
            putTLD[i].pointInputStream = null;
        }
        System.out.println("[" + new Date().toString() + "]: merge-sort completed!");
    }
    
    

    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    
    
    private static void buildOffsetTable()
    {
        // 造t/a轴小块大小的前缀和表
        for (int x = 1; x <= tSliceCount + 1; x++) {
            int s = 0;
            for (int y = 1; y <= N_ASLICE + 1; y++) {
                s += blockRecordCountPrefixSum.get(x, y);
                blockRecordCountPrefixSum.set(x, y, blockRecordCountPrefixSum.get(x - 1, y) + s);
            }
        }
        
        // 造a轴压缩点偏移表
        for (int aSlice2Id = 0; aSlice2Id < N_ASLICE2; aSlice2Id++) {
            long offsetL = 0;
            for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
                long t = aAxisCompressedPoint2ByteOffset.get(tSliceId, aSlice2Id);
                aAxisCompressedPoint2ByteOffset.set(tSliceId, aSlice2Id, offsetL);
                offsetL += t;
            }
        }
        
        // 造3号a轴压缩点偏移表
        for (int aSlice3Id = 0; aSlice3Id < N_ASLICE3; aSlice3Id++) {
            long offsetL = 0;
            for (int tSliceId = 0; tSliceId <= tSliceCount; tSliceId++) {
                long t = aAxisCompressedPoint3ByteOffset.get(tSliceId, aSlice3Id);
                aAxisCompressedPoint3ByteOffset.set(tSliceId, aSlice3Id, offsetL);
                offsetL += t;
            }
        }
    }
    
    static void buildIndex() throws IOException
    {
        // 计算a轴分割点
        calcPivotAxisA();
        
        // 进行外部排序：边排序边构建索引
        externalMergeSort();
        
        // 计算各个块在文件中的偏移
        for (int i = 0; i <= tSliceCount; i++) {
            if (i > 0) {
                tSliceRecordOffset[i] = tSliceRecordOffset[i - 1] + tSliceRecordCount[i - 1];
            }
//          System.out.println(String.format("t-slice %d: pivot=%d count=%d offset=%d", i, tSlicePivot[i], tSliceRecordCount[i], tSliceRecordOffset[i]));
        }
        
        // 建立内存内偏移表
        buildOffsetTable();
        
        // 建立A轴上的索引
        buildIndexAxisA();
        
        // 关闭用于写入的文件
        tAxisPointData.close();
        tAxisBodyData.close();
        aAxisIndexData.close();
        
        // 释放内存
        writeBuffer = null;
        writeBuffer2 = null;
        
        // 缓存数据
        System.gc();
        CachedCompressedPointAxisT.init(); // 会申请大量内存
        tAxisCompressedPointData.close();
    }
    
}
