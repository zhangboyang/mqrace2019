package io.openmessaging;

import static io.openmessaging.SliceManager.*;

import io.openmessaging.util.Compact2DIntArray;
import io.openmessaging.util.Compact2DLongArray;

public class IndexMetadata {

    /////// t分片的元数据
    static final int tSliceRecordCount[] = new int[N_TSLICE + 1];  // 每一个t分片内的记录数量
    static final int tSliceRecordOffset[] = new int[N_TSLICE + 1]; // 对应t分片在文件中的偏移（按记录数计）；实质上相当于tSliceRecordCount[]的前缀和
    
    
    /////// a-t小块的元数据
    // 原本是有三个数组的
    //   int blockRecordCount[][]        每一个小块内的记录数量
    //   int blockOffsetTableAxisT[][]   每一个小块在T-A索引文件中的偏移（按记录数计）
    //   int blockOffsetTableAxisA[][]   每一个小块在A-T索引文件中的偏移（按记录数计）
    // 为了节省内存，只保留一个blockRecordCount[][]的前缀和数组
    // 需要时，根据前缀和数组可以在O(1)时间内计算出其它数组对应位置的值
    static final Compact2DIntArray blockRecordCountPrefixSum = new Compact2DIntArray(N_TSLICE + 2, N_ASLICE + 2);
    static void incBlockOffsetTableCountPrefixSum(int x, int y) { blockRecordCountPrefixSum.inc(x + 1, y + 1); }
    static int getBlockOffsetTableCountPrefixSum(int x, int y) { return blockRecordCountPrefixSum.get(x + 1, y + 1); }
    static int blockOffsetTableAxisT(int x, int y)
    {
        return getBlockOffsetTableCountPrefixSum(x - 1, N_ASLICE) +
                (getBlockOffsetTableCountPrefixSum(x, y - 1) - getBlockOffsetTableCountPrefixSum(x - 1, y - 1));
    }
    static int blockOffsetTableAxisA(int x, int y)
    {
        return getBlockOffsetTableCountPrefixSum(tSliceCount, y - 1) +
                (getBlockOffsetTableCountPrefixSum(x - 1, y) - getBlockOffsetTableCountPrefixSum(x - 1, y - 1)) +
                (y * (tSliceCount + 1) + x);
    }
    
    
    /////// t轴压缩点索引的元数据
    static final long tSliceCompressedPointByteOffset[] = new long[N_TSLICE + 1]; // 对应t分片在文件中的偏移（按字节计）
    
    
    /////// a轴2号索引和3号索引的元数据
    //  ...BaseT[][] 是对应小块的t的基值（为了节省空间，小块内只存储每条记录的t相对上条记录的t的增量）
    //  ...ByteOffset[][] 是对应小块在各自文件中的偏移（按字节计）
    static final Compact2DLongArray aAxisCompressedPoint2BaseT = new Compact2DLongArray(N_TSLICE + 1, N_ASLICE2);
    static final Compact2DLongArray aAxisCompressedPoint2ByteOffset = new Compact2DLongArray(N_TSLICE + 1, N_ASLICE2);
    static final Compact2DLongArray aAxisCompressedPoint3BaseT = new Compact2DLongArray(N_TSLICE + 1, N_ASLICE3);
    static final Compact2DLongArray aAxisCompressedPoint3ByteOffset = new Compact2DLongArray(N_TSLICE + 1, N_ASLICE3);
    
}
