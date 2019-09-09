package io.openmessaging.util;

public class Compact2DIntArray {
    
    // Java自带的二维数组实际上是“数组套数组”
    // 这样会带来两个问题：
    //  （1）如果最右维较小，则对象本身开销占比会很大，浪费内存
    //  （2）如果最左维较大，则会产生很多低维数组对象，给GC造成很大压力
    // 此类通过使用一维数组来模拟二维数组，既节省内存，也减少GC时间
    
    final int array[];
    final int width;
    final int height;
    
    public Compact2DIntArray(int _height, int _width)
    {
        width = _width;
        height = _height;
        array = new int[width * height];
    }
    
    private void check(int x, int y)
    {
        assert 0 <= x && x < height && 0 <= y && y < width;
    }
    
    public int get(int x, int y)
    {
        check(x, y);
        return array[x * width + y];
    }
    
    public void set(int x, int y, int value)
    {
        check(x, y);
        array[x * width + y] = value;
    }
    
    public void inc(int x, int y)
    {
        check(x, y);
        array[x * width + y]++;
    }
    
    public void add(int x, int y, int value)
    {
        check(x, y);
        array[x * width + y] += value;
    }
}