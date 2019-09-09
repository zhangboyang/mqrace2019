package io.openmessaging;

import static io.openmessaging.FileStorageManager.tAxisCompressedPointChannel;
import static io.openmessaging.FileStorageManager.tAxisCompressedPointData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

class CachedCompressedPointAxisT {
    
    private static final int UNIT = 1000*1000*1000;
    
    // 使用堆内内存的缓存
    private static final ByteBuffer buffer1 = ByteBuffer.allocate((int)(UNIT*2.0));
    private static long offset1 = 1L*UNIT;
    
    // 使用堆外内存的缓存
    private static final ByteBuffer buffer2 = ByteBuffer.allocateDirect((int)(UNIT*1.7));
    private static long offset2 = 4L*UNIT;
    
    static {
        // 初次加载此类的时候才会分配内存
        System.out.println("[" + new Date() + "]: MyBufferedFile INIT!");
    }
    
    // 初始化缓存
    static void init() throws IOException
    {
        System.out.println("[" + new Date() + "]: MyBufferedFile load buffer1 started   offset1="+offset1);
        tAxisCompressedPointData.seek(offset1);   // 此处若用FileChannel.read则会导致临时directBuffer申请不到而报错
        tAxisCompressedPointData.read(buffer1.array());
        
        System.out.println("[" + new Date() + "]: MyBufferedFile load buffer2 started   offset2="+offset2);
        tAxisCompressedPointChannel.read(buffer2, offset2);
        
        System.out.println("[" + new Date() + "]: MyBufferedFile load done");
    }
    
    
    
    private static boolean hit1(long position, int size)
    {
        return offset1 <= position && position < offset1 + buffer1.capacity() && position + size <= offset1 + buffer1.capacity();
    }
    private static boolean hit2(long position, int size)
    {
        return offset2 <= position && position < offset2 + buffer2.capacity() && position + size <= offset2 + buffer2.capacity();
    }
    
    // 返回给定文件范围是否命中缓存
    static boolean hit(long position, int size)
    {
        return hit1(position, size) || hit2(position, size);
    }
    
    // 读取数据：若命中缓存则从缓存中取数据；否则从文件中取数据
    static void read(ByteBuffer buffer, long position) throws IOException
    {
        if (hit1(position, buffer.remaining())) {
            ByteBuffer tmpBuffer = buffer1.duplicate();
            tmpBuffer.position((int)(position - offset1));
            tmpBuffer.limit((int)(position - offset1 + buffer.remaining()));
            buffer.put(tmpBuffer);
            return;
        }
        if (hit2(position, buffer.remaining())) {
            ByteBuffer tmpBuffer = buffer2.duplicate();
            tmpBuffer.position((int)(position - offset2));
            tmpBuffer.limit((int)(position - offset2 + buffer.remaining()));
            buffer.put(tmpBuffer);
            return;
        }
        tAxisCompressedPointChannel.read(buffer, position);
    }
}