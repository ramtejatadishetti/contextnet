package edu.umass.cs.contextservice.configurator;

import edu.umass.cs.nio.interfaces.NodeConfig;

public class LoadAwareReplicatedSubspaceConfigurator<NodeIDType> 
									extends AbstractSubspaceConfigurator<NodeIDType> 
{
	public LoadAwareReplicatedSubspaceConfigurator(NodeConfig<NodeIDType> nodeConfig) 
	{
		super(nodeConfig);
		
	}

	@Override
	public void configureSubspaceInfo() 
	{
		
	}	
}