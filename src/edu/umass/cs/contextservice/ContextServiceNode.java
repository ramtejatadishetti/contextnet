package edu.umass.cs.contextservice;

import java.util.Iterator;

import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.schemes.QueryAllScheme;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;

public abstract class ContextServiceNode<NodeIDType>
{
	protected final NodeIDType myID; 
	
	protected final AbstractScheme<NodeIDType> contextservice;
	
	private boolean started = false;
	
	private final Object startMonitor = new Object();
	
	
	public ContextServiceNode(NodeIDType id, NodeConfig<NodeIDType> nc) throws Exception
	{
		this.myID = id;
		AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		ContextServiceLogger.getLogger().fine("\n\n node IP "+nc.getNodeAddress(this.myID)+
				" node Port "+nc.getNodePort(this.myID)+" nodeID "+this.myID);
		JSONNIOTransport<NodeIDType> jio = new JSONNIOTransport<NodeIDType>(this.myID,  nc, pd , true);
		
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(jio);
		
		ContextServiceLogger.getLogger().fine("Switch case started");
		
		switch(ContextServiceConfig.SCHEME_TYPE)
		{
			case HYPERSPACE_HASHING:
			{
				ContextServiceLogger.getLogger().fine("HYPERSPACE_HASHING started");
				this.contextservice = new HyperspaceHashing<NodeIDType>(nc, messenger);
				//this.contextservice = new QueryAllScheme<NodeIDType>(nc, messenger);
				ContextServiceLogger.getLogger().fine("HYPERSPACE_HASHING completed");
				break;
			}
			
			default:
			{
				ContextServiceLogger.getLogger().fine("null started");
				this.contextservice = null;
				break;
			}
		}
		
		Iterator<PacketType> packTypeIter = this.contextservice.getPacketTypes().iterator();
		
		while( packTypeIter.hasNext() )
		{
			PacketType pktType = packTypeIter.next();
			pd.register(pktType, this.contextservice);
		}
		messenger.addPacketDemultiplexer(pd);
		
		started = true;
		
		synchronized( startMonitor )
		{
			startMonitor.notify();
		}	
	}
	
	/**
	 * returns the context service
	 * @return
	 */
	public AbstractScheme<NodeIDType> getContextService()
	{
		return this.contextservice;
	}
	
	/**
	 * waits till the current node has started
	 */
	public void waitToFinishStart()
	{
		synchronized( startMonitor )
		{
			while( !started )
			{
				try 
				{
					startMonitor.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}	
}