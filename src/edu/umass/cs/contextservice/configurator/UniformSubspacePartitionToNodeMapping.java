package edu.umass.cs.contextservice.configurator;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;

/**
 * This scheme assigns regions to nodes in near uniform manner.
 * If there are H attributes and P partitions, then there are 
 * P^{H} regions. This scheme checks 10 consecutive Ps, such that
 * P^{H} = N and chooses the one that gives highest Jain's fairness index.
 * we choose 10 because this can generate most of the multiples.
 * @author ayadav
 */
public class UniformSubspacePartitionToNodeMapping
{
	protected final HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	protected final ExecutorService nodeES;
	protected final HyperspaceDB hyperspaceDB;
	
	public UniformSubspacePartitionToNodeMapping(
			HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap, 
			ExecutorService nodeES, HyperspaceDB hyperspaceDB)
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.nodeES = nodeES;
		this.hyperspaceDB = hyperspaceDB;
	}
	
	
}