package io.openmessaging;

import java.io.IOException;
import java.util.*;
import static io.openmessaging.IndexWriter.*;
import static io.openmessaging.PutMessageProcessor.*;
import static io.openmessaging.GetMessageProcessor.*;
import static io.openmessaging.AvgQueryProcessor.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {
    
    // 状态变量
    private static volatile int state = 0;
    private static final Object stateLock = new Object();
    
    // 添加用于打印统计信息的钩子
    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                printAvgQueryStatistics();
            }
        });
    }

    
    
    @Override
    public void put(Message message) {

        if (state == 0) {
            synchronized (stateLock) {
                if (state == 0) {
                    System.out.println("[" + new Date() + "]: put() started");
                    
                    boolean assertsEnabled = false;
                    assert assertsEnabled = true;
                    
                    System.out.println("assertEnabled=" + assertsEnabled);

                    state = 1;
                }
            }
        }
        
        try {
            doPutMessage(message);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    
    
    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {

        if (state == 1) {
            synchronized (stateLock) {
                if (state == 1) {
                    System.out.println("[" + new Date() + "]: getMessage() started");

                    try {
                        
                        flushPutBuffer();
                        buildIndex();
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                        state = -1;
                        System.exit(-1);
                    }

                    System.gc();
                    state = 2;
                }
            }
        }
        

        try {
            return doGetMessage(aMin, aMax, tMin, tMax);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        
        try {
            return doGetAvgValue(aMin, aMax, tMin, tMax);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0;
    }
}
