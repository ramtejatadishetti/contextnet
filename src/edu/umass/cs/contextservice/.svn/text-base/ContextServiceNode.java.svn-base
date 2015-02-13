package edu.umass.cs.contextservice;

import java.io.IOException;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.schemes.ContextNetScheme;
import edu.umass.cs.contextservice.schemes.MercuryScheme;
import edu.umass.cs.contextservice.schemes.QueryAllScheme;
import edu.umass.cs.contextservice.schemes.ReplicateAllScheme;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;

public abstract class ContextServiceNode<NodeIDType>
{
	protected final NodeIDType myID; // only needed for app debugging in createAppCoordinator
	//protected final ActiveReplica<NodeIDType> activeReplica;
	protected final AbstractScheme<NodeIDType> contextservice;
	
	//protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();
	
	public ContextServiceNode(NodeIDType id, InterfaceNodeConfig<NodeIDType> nc) throws IOException
	{
		this.myID = id;
		AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//ContextServicePacketDemultiplexer pd;
		
		System.out.println("\n\n node IP "+nc.getNodeAddress(this.myID)+
				" node Port "+nc.getNodePort(this.myID)+" nodeID "+this.myID);
		JSONNIOTransport<NodeIDType> jio = new JSONNIOTransport<NodeIDType>(this.myID,  nc, pd , true);
		
		//new Thread(jio).start();
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(jio.enableStampSenderInfo());
		
		//JSONMessenger<NodeIDType> messenger = new JSONMessenger<NodeIDType>((new JSONNIOTransport<NodeIDType>(this.myID, 
		//				nc)).enableStampSenderInfo());
		//this.activeReplica = new ActiveReplica<NodeIDType>(createAppCoordinator(), nc, messenger);
		//if(ContextServiceConfig.SCHEME_TYPE == ContextServiceConfig.SCHEME_TYPE.CONTEXTNET)
		switch(ContextServiceConfig.SCHEME_TYPE)
		{
			case CONTEXTNET:
			{
				this.contextservice = new ContextNetScheme<NodeIDType>(nc, messenger);
				break;
			}
			case REPLICATE_ALL:
			{
				this.contextservice = new ReplicateAllScheme<NodeIDType>(nc, messenger);
				break;
			}
			case QUERY_ALL:
			{
				this.contextservice = new QueryAllScheme<NodeIDType>(nc, messenger);
				break;
			}
			case MERCURY:
			{
				this.contextservice = new MercuryScheme<NodeIDType>(nc, messenger);
				break;
			}
			default:
			{
				this.contextservice = null;
				break;
			}
		}
		//((ContextServicePacketDemultiplexer)pd).setPacketDemux(this.contextservice);
		//pd = new ContextServicePacketDemultiplexer(contextservice);
		
		//pd.register(this.activeReplica.getPacketTypes(), this.activeReplica); // includes app packets
		pd.register(this.contextservice.getPacketTypes().toArray(), this.contextservice);
		messenger.addPacketDemultiplexer(pd);
		
		// add delay so that messages are not lost in the 
		// beginning
		try
		{
			Thread.sleep(60000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		//packet types registered, now spawn the task and start the system
		this.contextservice.spawnTheTask();
	}
	
	/**
	 * prints the value node and meta data 
	 * information
	 */
	public void printNodeState()
	{
		this.contextservice.printTheStateAtNode();
	}
	
	/**
	 * returns the context service
	 * @return
	 */
	public AbstractScheme<NodeIDType> getContextService()
	{
		return this.contextservice;
	}
}