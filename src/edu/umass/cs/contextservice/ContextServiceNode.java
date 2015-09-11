package edu.umass.cs.contextservice;

import java.io.IOException;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.schemes.ContextNetScheme;
import edu.umass.cs.contextservice.schemes.HyperdexScheme;
import edu.umass.cs.contextservice.schemes.HyperdexSchemeNew;
import edu.umass.cs.contextservice.schemes.MercuryNewScheme;
import edu.umass.cs.contextservice.schemes.MercuryScheme;
import edu.umass.cs.contextservice.schemes.MercurySchemeConsistent;
import edu.umass.cs.contextservice.schemes.QueryAllScheme;
import edu.umass.cs.contextservice.schemes.ReplicateAllScheme;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;

public abstract class ContextServiceNode<NodeIDType>
{
	protected final NodeIDType myID; // only needed for app debugging in createAppCoordinator
	//protected final ActiveReplica<NodeIDType> activeReplica;
	protected final AbstractScheme<NodeIDType> contextservice;
	
	//protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();
	
	public ContextServiceNode(NodeIDType id, InterfaceNodeConfig<NodeIDType> nc) throws IOException
	{
		this.myID = id;
		AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		System.out.println("\n\n node IP "+nc.getNodeAddress(this.myID)+
				" node Port "+nc.getNodePort(this.myID)+" nodeID "+this.myID);
		JSONNIOTransport<NodeIDType> jio = new JSONNIOTransport<NodeIDType>(this.myID,  nc, pd , true);
		
		//new Thread(jio).start();
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(jio);
		
		//JSONMessenger<NodeIDType> messenger = new JSONMessenger<NodeIDType>((new JSONNIOTransport<NodeIDType>(this.myID, 
		//				nc)).enableStampSenderInfo());
		//this.activeReplica = new ActiveReplica<NodeIDType>(createAppCoordinator(), nc, messenger);
		//if(ContextServiceConfig.SCHEME_TYPE == ContextServiceConfig.SCHEME_TYPE.CONTEXTNET)
		System.out.println("Switch case started");
		
		switch(ContextServiceConfig.SCHEME_TYPE)
		{
			case CONTEXTNET:
			{
				System.out.println("CONTEXTNET started");
				this.contextservice = new ContextNetScheme<NodeIDType>(nc, messenger);
				break;
			}
			case REPLICATE_ALL:
			{
				System.out.println("REPLICATE_ALL started");
				this.contextservice = new ReplicateAllScheme<NodeIDType>(nc, messenger);
				break;
			}
			case QUERY_ALL:
			{
				System.out.println("QUERY_ALL started");
				this.contextservice = new QueryAllScheme<NodeIDType>(nc, messenger);
				break;
			}
			case MERCURY:
			{
				System.out.println("MERCURY started");
				this.contextservice = new MercuryScheme<NodeIDType>(nc, messenger);
				break;
			}
			case HYPERDEX:
			{
				System.out.println("HYPERDEX started");
				this.contextservice = new HyperdexSchemeNew<NodeIDType>(nc, messenger);
				break;
			}
			case MERCURYNEW:
			{
				System.out.println("MERCURYNEW started");
				this.contextservice = new MercuryNewScheme<NodeIDType>(nc, messenger);
				break;
			}
			case MERCURYCONSISTENT:
			{
				System.out.println("MERCURYCONSISTENT started");
				this.contextservice = new MercurySchemeConsistent<NodeIDType>(nc, messenger);
				break;
			}
			default:
			{
				System.out.println("null started");
				this.contextservice = null;
				break;
			}
		}
		
		//((ContextServicePacketDemultiplexer)pd).setPacketDemux(this.contextservice);
		//pd = new ContextServicePacketDemultiplexer(contextservice);
		
		//pd.register(this.activeReplica.getPacketTypes(), this.activeReplica); // includes app packets
		pd.register(this.contextservice.getPacketTypes().toArray(), this.contextservice);
		messenger.addPacketDemultiplexer(pd);
		
		//FIXME: remove this sleep, it is there because 
		// we wait for all nodes to start before sending out initialize messages.
		// may be by ack and retry sleep can be removed.
		try
		{
			Thread.sleep(60000);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		// add delay so that messages are not lost in the 
		// beginning
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