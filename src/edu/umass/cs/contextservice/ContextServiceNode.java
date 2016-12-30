package edu.umass.cs.contextservice;

import java.util.Iterator;

import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.schemes.QueryAllScheme;
import edu.umass.cs.contextservice.schemes.RegionMappingBasedScheme;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;

public abstract class ContextServiceNode
{
	protected final Integer myID; 
	
	protected final AbstractScheme contextservice;
	
	private boolean started = false;
	
	private final Object startMonitor = new Object();
	
	
	public ContextServiceNode(Integer id, CSNodeConfig nc) throws Exception
	{
		this.myID = id;
		AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		ContextServiceLogger.getLogger().fine("\n\n node IP "+nc.getNodeAddress(this.myID)+
				" node Port "+nc.getNodePort(this.myID)+" nodeID "+this.myID);
		JSONNIOTransport<Integer> jio = new JSONNIOTransport<Integer>(this.myID,  nc, pd , true);
		
		JSONMessenger<Integer> messenger = 
			new JSONMessenger<Integer>(jio);
		
		
		if(ContextServiceConfig.QUERY_ALL_ENABLED)
		{
			this.contextservice = new QueryAllScheme(nc, messenger);
		}
		else
		{
			this.contextservice = new RegionMappingBasedScheme(nc, messenger);
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
	public AbstractScheme getContextService()
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