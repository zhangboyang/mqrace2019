package io.openmessaging.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ValueCompressor {
    
    // 数值压缩器：用类似UTF-8的变长编码来压缩一个64位整数
    // 压缩后的数据最长可能需要9字节
    
    // 编码说明：
    //    0xxxxxxx   1字节-7bit
    //    10xxxxxx xxxxxxxx   2字节-14bit
    //    110xxxxx xxxxxxxx xxxxxxxx   3字节-21bit
    //    1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx   4字节-28bit
    //    11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   5字节-35bit
    //    111110xx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   6字节-42bit
    //    1111110x xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx  7字节-49bit
    //    11111110 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   8字节-56bit
    //    11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx  9字节-64bit
    
    
    public static void putToBuffer(ByteBuffer buffer, long value)
    {
        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;

        if ((value & 0x7fL) == value) {
            buffer.put((byte)(value));
        } else if ((value & 0x3fffL) == value) {
            buffer.put((byte)((value >>> 8) | 0x80));
            buffer.put((byte)(value));
        } else if ((value & 0x1fffffL) == value) {
            buffer.put((byte)((value >>> 16) | 0xc0));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else if ((value & 0xfffffffL) == value) {
            buffer.put((byte)((value >>> 24) | 0xe0));
            buffer.put((byte)(value >>> 16));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else if ((value & 0x7ffffffffL) == value) {
            buffer.put((byte)((value >>> 32) | 0xf0));
            buffer.put((byte)(value >>> 24));
            buffer.put((byte)(value >>> 16));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else if ((value & 0x3ffffffffffL) == value) {
            buffer.put((byte)((value >>> 40) | 0xf8));
            buffer.put((byte)(value >>> 32));
            buffer.put((byte)(value >>> 24));
            buffer.put((byte)(value >>> 16));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else if ((value & 0x1ffffffffffffL) == value) {
            buffer.put((byte)((value >>> 48) | 0xfc));
            buffer.put((byte)(value >>> 40));
            buffer.put((byte)(value >>> 32));
            buffer.put((byte)(value >>> 24));
            buffer.put((byte)(value >>> 16));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else if ((value & 0xffffffffffffffL) == value) {
            buffer.put((byte)0xfe);
            buffer.put((byte)(value >>> 48));
            buffer.put((byte)(value >>> 40));
            buffer.put((byte)(value >>> 32));
            buffer.put((byte)(value >>> 24));
            buffer.put((byte)(value >>> 16));
            buffer.put((byte)(value >>> 8));
            buffer.put((byte)(value));
        } else {
            buffer.put((byte)0xff);
            buffer.putLong(value);
        }
    }
    
    public static int getLengthByValue(long value)
    {
        if ((value & 0x7fL) == value) {
            return 1;
        } else if ((value & 0x3fffL) == value) {
            return 2;
        } else if ((value & 0x1fffffL) == value) {
            return 3;
        } else if ((value & 0xfffffffL) == value) {
            return 4;
        } else if ((value & 0x7ffffffffL) == value) {
            return 5;
        } else if ((value & 0x3ffffffffffL) == value) {
            return 6;
        } else if ((value & 0x1ffffffffffffL) == value) {
            return 7;
        } else if ((value & 0xffffffffffffffL) == value) {
            return 8;
        } else {
            return 9;
        }
    }
    
    public static long getFromBuffer(ByteBuffer buffer)
    {
        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;
        
        long value;
        long firstbyte = ((int)buffer.get()) & 0xff;
        
        if (firstbyte < 0x80) {
            value = firstbyte;
        } else if (firstbyte < 0xc0) {
            value = (firstbyte & 0x3f) << 8; 
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xe0) {
            value = (firstbyte & 0x1f) << 16; 
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xf0) {
            value = (firstbyte & 0x0f) << 24; 
            value |= ((long)buffer.get() & 0xff) << 16;
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xf8) {
            value = (firstbyte & 0x07) << 32; 
            value |= ((long)buffer.get() & 0xff) << 24;
            value |= ((long)buffer.get() & 0xff) << 16;
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xfc) {
            value = (firstbyte & 0x03) << 40; 
            value |= ((long)buffer.get() & 0xff) << 32;
            value |= ((long)buffer.get() & 0xff) << 24;
            value |= ((long)buffer.get() & 0xff) << 16;
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xfe) {
            value = (firstbyte & 0x01) << 48; 
            value |= ((long)buffer.get() & 0xff) << 40;
            value |= ((long)buffer.get() & 0xff) << 32;
            value |= ((long)buffer.get() & 0xff) << 24;
            value |= ((long)buffer.get() & 0xff) << 16;
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else if (firstbyte < 0xff) {
            value = ((long)buffer.get() & 0xff) << 48;
            value |= ((long)buffer.get() & 0xff) << 40;
            value |= ((long)buffer.get() & 0xff) << 32;
            value |= ((long)buffer.get() & 0xff) << 24;
            value |= ((long)buffer.get() & 0xff) << 16;
            value |= ((long)buffer.get() & 0xff) << 8;
            value |= ((long)buffer.get() & 0xff);
        } else {
            value = buffer.getLong();
        }
        
        return value;
    }
}
