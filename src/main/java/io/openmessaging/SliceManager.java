package io.openmessaging;

import static io.openmessaging.util.Util.*;

import java.util.ArrayList;
import java.util.Collections;



public class SliceManager {
    
    static final int MAXMSG = 2100000000;    // 最大总消息条数
    static int globalTotalRecords = 0;       // 实际总消息条数
    
    
    
    static final int N_TSLICE = 2500000;           // t轴最大分片数
    static int tSliceCount = 0;                    // t轴实际分片数
    
    static final int TSLICE_INTERVAL = MAXMSG / N_TSLICE;  // t轴分片间隔
    
    static final long tSlicePivot[] = new long[N_TSLICE + 1];  // t轴分割点
    


    static final int N_ASLICE = 40;                // a轴实际分片数 
    static final int N_ASLICE2 = 8;                // a轴2号索引实际分片数
    static final int N_ASLICE3 = N_ASLICE2 + 1;    // a轴3号索引实际分片数
    
    static final long aSlicePivot[] = new long[N_ASLICE + 1];     // a轴分割点
    static final long aSlice2Pivot[] = new long[N_ASLICE2 + 1];   // a轴2号索引分割点
    static final long aSlice3Pivot[] = new long[N_ASLICE3 + 1];   // a轴3号索引分割点
    
    
    
    
    // 通过二分查找的方式，找给定值所对应的分片ID号
    static int findSliceT(long tValue)
    {
        int l = lowerBound(tSlicePivot, 0, tSliceCount, tValue);
        assert tSlicePivot[l] <= tValue && tValue < tSlicePivot[l + 1];
        return l;
    }
    static int findSliceA(long aValue)
    {
        int l = lowerBound(aSlicePivot, 0, N_ASLICE, aValue);
        assert aSlicePivot[l] <= aValue && aValue < aSlicePivot[l + 1];
        return l;
    }
    static int findSliceA2(long aValue)
    {
        int l = lowerBound(aSlice2Pivot, 0, N_ASLICE2, aValue);
        assert aSlice2Pivot[l] <= aValue && aValue < aSlice2Pivot[l + 1];
        return l;
    }
    static int findSliceA3(long aValue)
    {
        int l = lowerBound(aSlice3Pivot, 0, N_ASLICE3, aValue);
        assert aSlice3Pivot[l] <= aValue && aValue < aSlice3Pivot[l + 1];
        return l;
    }
    
    
    
    static long globalMaxA = Long.MIN_VALUE; // 全局a最大值
    static long globalMinA = Long.MAX_VALUE; // 全局a最小值
    static final ArrayList<Long> aSamples = new ArrayList<Long>();   // 用于计算a轴分割点的抽样数据

    // 根据抽样数据计算a轴分割点
    static void calcPivotAxisA()
    {
        
        // 计算样本的n分位数，作为a轴上的分割点
        Collections.sort(aSamples);
        for (int i = 0; i < N_ASLICE; i++) {
            aSlicePivot[i] = aSamples.get(aSamples.size() / N_ASLICE * i).longValue();
        }
        aSlicePivot[0] = globalMinA;
        aSlicePivot[N_ASLICE] = Long.MAX_VALUE;
        for (int i = 0; i <= N_ASLICE; i++) {
            System.out.println(String.format("aSlicePivot[%d]=%d", i, aSlicePivot[i]));
        }
        
        // 计算2号a索引的分割点
        for (int i = 0; i < N_ASLICE2; i++) {
            aSlice2Pivot[i] = aSamples.get(aSamples.size() / N_ASLICE2 * i).longValue();
        }
        aSlice2Pivot[0] = globalMinA;
        aSlice2Pivot[N_ASLICE2] = Long.MAX_VALUE;
        for (int i = 0; i <= N_ASLICE2; i++) {
            System.out.println(String.format("aSlice2Pivot[%d]=%d", i, aSlice2Pivot[i]));
        }
        
        // 计算3号a索引的分割点
        for (int i = 0; i < N_ASLICE3 - 1; i++) {
            aSlice3Pivot[i + 1] = aSamples.get(aSamples.size() / (N_ASLICE3 - 1) / 3 + aSamples.size() / (N_ASLICE3 - 1) * i).longValue();
        }
        aSlice3Pivot[0] = globalMinA;
        aSlice3Pivot[N_ASLICE3] = Long.MAX_VALUE;
        for (int i = 0; i <= N_ASLICE3; i++) {
            System.out.println(String.format("aSlice3Pivot[%d]=%d", i, aSlice3Pivot[i]));
        }
    }

}
