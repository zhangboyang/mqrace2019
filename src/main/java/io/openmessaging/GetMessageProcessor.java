package io.openmessaging;

import static io.openmessaging.FileStorageManager.tAxisBodyChannel;
import static io.openmessaging.PutMessageProcessor.nPutThread;
import static io.openmessaging.PutMessageProcessor.putTLD;
import static io.openmessaging.SliceManager.findSliceT;
import static io.openmessaging.util.Util.pointInRect;
import static io.openmessaging.util.Util.tComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.openmessaging.util.ValueCompressor;

public class GetMessageProcessor {

	static ArrayList<Message> doGetMessage(long aMin, long aMax, long tMin, long tMax) throws IOException
	{
		
		ArrayList<Message> result = new ArrayList<Message>();
    	
    	int tSliceLow = findSliceT(tMin);
    	int tSliceHigh = findSliceT(tMax);
    	int nThread = nPutThread;
    	
		ByteBuffer tSliceLowOffsetBuffer = ByteBuffer.allocate(nThread * 20);
		tSliceLowOffsetBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisBodyChannel.read(tSliceLowOffsetBuffer, (long)tSliceLow * nThread * 20);
		
		ByteBuffer tSliceHighOffsetBuffer = ByteBuffer.allocate(nThread * 20);
		tSliceHighOffsetBuffer.order(ByteOrder.LITTLE_ENDIAN);
		tAxisBodyChannel.read(tSliceHighOffsetBuffer, (long)(tSliceHigh + 1) * nThread * 20); // exclusive
		
		tSliceLowOffsetBuffer.position(0);
		tSliceHighOffsetBuffer.position(0);
		for (int threadId = 0; threadId < nThread; threadId++) {
			
			int recordOffset = tSliceLowOffsetBuffer.getInt();
			int nRecord = tSliceHighOffsetBuffer.getInt() - recordOffset;
			long byteOffset = tSliceLowOffsetBuffer.getLong();
			int nBytes = (int)(tSliceHighOffsetBuffer.getLong() - byteOffset);
			long t = tSliceLowOffsetBuffer.getLong();
			tSliceHighOffsetBuffer.getLong();
			
			ByteBuffer zpBuffer = ByteBuffer.allocate(nBytes);
			zpBuffer.order(ByteOrder.LITTLE_ENDIAN);
    		ByteBuffer bodyBuffer = ByteBuffer.allocate(nRecord * 34);
    		bodyBuffer.order(ByteOrder.LITTLE_ENDIAN);
    		
    		putTLD[threadId].bodyChannel.read(bodyBuffer, (long)recordOffset * 34);
    		putTLD[threadId].zpChannel.read(zpBuffer, byteOffset);
			
    		zpBuffer.position(0);

    		for (int recordId = 0; recordId < nRecord; recordId++) {
				
				t += ValueCompressor.getFromBuffer(zpBuffer);
				long a = ValueCompressor.getFromBuffer(zpBuffer);
				
				if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
					byte body[] = new byte[34];
					bodyBuffer.position(recordId * 34);
					bodyBuffer.get(body);
					result.add(new Message(a, t, body));
					assert ByteBuffer.wrap(body).getLong() == t;
				}
			}
			
			assert !zpBuffer.hasRemaining();

		}
		
		Collections.sort(result, tComparator);
		
		return result;
	}
}
