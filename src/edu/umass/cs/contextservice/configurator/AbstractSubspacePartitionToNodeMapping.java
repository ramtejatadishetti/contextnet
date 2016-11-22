package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;

/**
 * This is an abstract class for subspace partition, region in a subspace, to node 
 * mapping. This class generates the mapping and stores it in the database.
 * Mapping size is usually large so that's why it is not kept in memory.
 * Sub classes of this class implement various schemes like random mapping, where
 * regions are mapped randomly to nodes. Contigous mapping, where contigous regions are
 * mapped to same nodes.
 * @author ayadav
 */
public abstract class AbstractSubspacePartitionToNodeMapping
{
	protected final HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	protected final ExecutorService nodeES;
	protected final HyperspaceDB hyperspaceDB;
	
	public AbstractSubspacePartitionToNodeMapping(
			HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap, 
			ExecutorService nodeES, HyperspaceDB hyperspaceDB)
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.nodeES = nodeES;
		this.hyperspaceDB = hyperspaceDB;
	}
	
	public abstract void generateAndStoreSubspacePartitionsInDB();
}