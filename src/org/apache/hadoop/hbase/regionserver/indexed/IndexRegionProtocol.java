package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface IndexRegionProtocol extends CoprocessorProtocol {

	void index(boolean forceRebuild) throws IOException;

}
