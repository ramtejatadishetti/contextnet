package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;

/**
 * This class implements contigous regions to node mapping. An example to demonstrate
 * the scheme implemented. For 100 nodes, with 4 attrbutes.
 * Context service creates 4^{4} regions, as 3^{4} is smaller than 100. 
 * But 
 * @author ayadav                                                                                                                                                                                                                                                                                                                                                                            
 */
public class ContigousSubspacePartitionToNodeMapping
{
	protected final HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	protected final ExecutorService nodeES;
	protected final HyperspaceDB hyperspaceDB;
	
	public ContigousSubspacePartitionToNodeMapping(
			HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap, 
			ExecutorService nodeES, HyperspaceDB hyperspaceDB)
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.nodeES = nodeES;
		this.hyperspaceDB = hyperspaceDB;
	}
	
	public void generateAndStoreSubspacePartitionsInDB()
	{
		
	}
}