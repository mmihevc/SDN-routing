package edu.brown.cs.sdn.apps.sps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.brown.cs.sdn.apps.util.Host;
import edu.brown.cs.sdn.apps.util.SwitchCommands;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.packet.Ethernet;

import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.OFOXMFieldType;
import org.openflow.protocol.OFMatchField;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;

import java.util.*;

public class ShortestPathSwitching implements IFloodlightModule, IOFSwitchListener, 
		ILinkDiscoveryListener, IDeviceListener, InterfaceShortestPathSwitching
{
	public static final String MODULE_NAME = ShortestPathSwitching.class.getSimpleName();
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;

    // Interface to link discovery service
    private ILinkDiscoveryService linkDiscProv;

    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    private byte table;
    
    // Map of hosts to devices
    private Map<IDevice,Host> knownHosts;

	/**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.linkDiscProv = context.getServiceImpl(ILinkDiscoveryService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        this.knownHosts = new ConcurrentHashMap<IDevice,Host>();
        
        /*********************************************************************/
        /* TODO: Initialize other class variables, if necessary              */
		/* might need to initialize for shortest path
        /* clear table/rebuild table every time 
        /*********************************************************************/
	
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.linkDiscProv.addListener(this);
		this.deviceProv.addListener(this);
		
		/*********************************************************************/
		/* TODO: Perform other tasks, if necessary                           */
		
		/*********************************************************************/
	}
	
	/**
	 * Get the table in which this application installs rules.
	 */
	public byte getTable()
	{ return this.table; }
	
    /**
     * Get a list of all known hosts in the network.
     */
    private Collection<Host> getHosts()
    { return this.knownHosts.values(); }
	
    /**
     * Get a map of all active switches in the network. Switch DPID is used as
     * the key.
     */
	private Map<Long, IOFSwitch> getSwitches()
    { return floodlightProv.getAllSwitchMap(); }
	
    /**
     * Get a list of all active links in the network.
     */
    private Collection<Link> getLinks()
    { return linkDiscProv.getLinks().keySet(); }

    /**
     * Event handler called when a host joins the network.
     * @param device information about the host
     */

	public IOFSwitch getMinSwitch(HashMap<IOFSwitch, Integer> pq) {
		Integer min = Integer.MAX_VALUE;
		IOFSwitch minSwitch = null;

		for (IOFSwitch node: pq.keySet()) {
			if (pq.get(node) <= min) {
				min = pq.get(node);
				minSwitch = node;
			}
		}
		return minSwitch;
	}

	public HashMap<IOFSwitch, HashMap<IOFSwitch, IOFSwitch>> dykstras() {
		Map<Long, IOFSwitch> switches = getSwitches();
		Collection<Link> links = getLinks();
		HashMap<IOFSwitch, HashMap<IOFSwitch, IOFSwitch>> sp = new HashMap<IOFSwitch, HashMap<IOFSwitch, IOFSwitch>>();

		for (IOFSwitch node: switches.values()) {

			//Hashmap of the distance between switches
			HashMap<IOFSwitch, Integer> distance = new HashMap<IOFSwitch, Integer>();

			//priority queue to track switches that have not been processed yet
			HashMap<IOFSwitch, Integer> priorityQueue = new HashMap<IOFSwitch, Integer>();

			HashMap<IOFSwitch, IOFSwitch> prevNodes = new HashMap<IOFSwitch, IOFSwitch>();

			for (IOFSwitch n : switches.values()) {
				distance.put(n, Integer.MAX_VALUE);
				priorityQueue.put(n, Integer.MAX_VALUE);
				prevNodes.put(n, null);
			}

			distance.put(node, 0);
			priorityQueue.put(node, 0);

			for (Link link : links) {
				IOFSwitch start = switches.get(link.getSrc());
				IOFSwitch destination = switches.get(link.getDst());

				if (start == node) {
					distance.put(destination, 1);
					priorityQueue.put(destination, 1);
					prevNodes.put(destination, start);
				}
			}

			Set<IOFSwitch> neighbors = new HashSet<IOFSwitch>();
			IOFSwitch minSwitch = null;

			while (!priorityQueue.isEmpty()) {
				minSwitch = getMinSwitch(priorityQueue);
				priorityQueue.remove(minSwitch);
				neighbors.add(minSwitch);

				for (Link link: links) {
					IOFSwitch start = switches.get(link.getSrc());
					IOFSwitch neighbor = switches.get(link.getDst());

					if (start == minSwitch && !neighbors.contains(neighbor)) {
						if (distance.get(minSwitch) + 1 < distance.get(neighbor)) {
							distance.put(neighbor, distance.get(neighbor) + 1);
							priorityQueue.put(neighbor, distance.get(neighbor) + 1);
							prevNodes.put(neighbor, minSwitch);
						}
					}
				}
			}
			sp.put(node, prevNodes);
		}
		return sp;
	}

	public OFMatch addMatch(Host host) {
		OFMatch ofMatch = new OFMatch();
		OFMatchField etherType = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_IPv4);
		OFMatchField macAddr = new OFMatchField(OFOXMFieldType.ETH_DST, Ethernet.toByteArray(host.getMACAddress()));
		ofMatch.setMatchFields(Arrays.asList(etherType, macAddr));
		return ofMatch;
	}

	public OFActionOutput getActionOutput(Host host, IOFSwitch currentSwitch) {
		IOFSwitch hostSwitch = host.getSwitch();
		OFActionOutput ofActionOutput = new OFActionOutput();

		log.info(String.format("currentSwitch == hostSwitch: %b", currentSwitch.getId() == hostSwitch.getId()));

		if (currentSwitch.getId() == hostSwitch.getId()) {
			log.info(String.format("Port is %s ", host.getPort()));
			ofActionOutput.setPort(host.getPort());
		} 
		else {
			Collection<Link> links = getLinks();
			HashMap<IOFSwitch, HashMap<IOFSwitch, IOFSwitch>> shortestPath = dykstras();

			IOFSwitch neighbor = shortestPath.get(hostSwitch).get(currentSwitch);
			
			for (Link link: links) {
				if (currentSwitch.getId() == link.getSrc() && neighbor.getId() == link.getDst()) {
	
					log.info(String.format("NEIGHBOR Port is %s ", link.getSrcPort()));
					ofActionOutput.setPort(link.getSrcPort());
				}
			}
		}

		return ofActionOutput;
	}

	public void installCommand(Host host, IOFSwitch currentSwitch, OFMatch match, byte table) {
		OFInstructionApplyActions actions = new OFInstructionApplyActions(Collections.<OFAction>singletonList(getActionOutput(host, currentSwitch)));
		SwitchCommands.installRule(currentSwitch, table, SwitchCommands.DEFAULT_PRIORITY, match, Collections.<OFInstruction>singletonList(actions));
	}

	public void buildAllRules() {
		Collection<Host> hosts = getHosts();
		for (Host host: hosts) {
			//check if Ipv4 host is null
			if (host.getIPv4Address() != null)
				buildRule(host); 
			
		}
	}

	public void buildRule(Host host) {
		log.info(String.format("Host is attached to switch: %b", host.isAttachedToSwitch()));

		if (host.isAttachedToSwitch()) {
			OFMatch match = addMatch(host);

			for (IOFSwitch currentSwitch: getSwitches().values()) {
				installCommand(host, currentSwitch, match, this.table);
			}
			
		}
	}


	public void clearAllRules() {
		Collection<Host> hosts = getHosts();
		for (Host host: hosts) {
			clearRule(host);
		}
	}

	public void clearRule(Host host) {
		Map<Long, IOFSwitch> switches = getSwitches();
		OFMatch ofMatch = addMatch(host);

		for (IOFSwitch node: switches.values()) {
			SwitchCommands.removeRules(node, this.table, ofMatch);
		}
	}


	@Override
	public void deviceAdded(IDevice device) 
	{
		Host host = new Host(device, this.floodlightProv);

		log.info(String.format("HOST NAME: %s and HOST IP ADDRESS: %d", host.getName(), host.getIPv4Address()));

		// We only care about a new host if we know its IP
		if (host.getIPv4Address() != null)
		{
			log.info(String.format("Host %s added", host.getName()));
			this.knownHosts.put(device, host);
			
			/*****************************************************************/
			/* TODO: Update routing: add rules to route to new host          */
			/* call addMatch here (match is rules to route to host)          */
			/* device discovery, init routine 
			/*****************************************************************/
			buildRule(host);
		}
	}

	/**
     * Event handler called when a host is no longer attached to a switch.
     * @param device information about the host
     */
	@Override
	public void deviceRemoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);

		log.info(String.format("HOST BEING REMOVED: %s", host.getName()));

		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}
		
		log.info(String.format("Host %s is no longer attached to a switch", 
				host.getName()));
		
		/*********************************************************************/
		/* TODO: Update routing: remove rules to route to host               */
		
		/*********************************************************************/
		//destroy
		clearRule(host);
		
	}

	/**
     * Event handler called when a host moves within the network.
     * @param device information about the host
     */
	@Override
	public void deviceMoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}
		
		if (!host.isAttachedToSwitch())
		{
			this.deviceRemoved(device);
			return;
		}
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));
		
		/*********************************************************************/
		/* TODO: Update routing: change rules to route to host               */
		
		/*********************************************************************/
		//destory rule/build rule
		clearRule(host);
		buildRule(host);
	}
	
    /**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override		
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */

		/*********************************************************************/
		//destroy then build all
		clearAllRules();
		buildAllRules();
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d removed", switchId));
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		
		/*********************************************************************/
		clearAllRules();
		buildAllRules();
	}

	/**
	 * Event handler called when multiple links go up or down.
	 * @param updateList information about the change in each link's state
	 */
	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) 
	{
		for (LDUpdate update : updateList)
		{
			// If we only know the switch & port for one end of the link, then
			// the link must be from a switch to a host
			if (0 == update.getDst())
			{
				log.info(String.format("Link s%s:%d -> host updated", 
					update.getSrc(), update.getSrcPort()));
			}
			// Otherwise, the link is between two switches
			else
			{
				log.info(String.format("Link s%s:%d -> %s:%d updated", 
					update.getSrc(), update.getSrcPort(),
					update.getDst(), update.getDstPort()));
			}
		}
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		
		/*********************************************************************/
		buildAllRules();
	}

	/**
	 * Event handler called when link goes up or down.
	 * @param update information about the change in link state
	 */
	@Override
	public void linkDiscoveryUpdate(LDUpdate update) 
	{ this.linkDiscoveryUpdate(Arrays.asList(update)); }
	
	/**
     * Event handler called when the IP address of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceIPV4AddrChanged(IDevice device) 
	{ this.deviceAdded(device); }

	/**
     * Event handler called when the VLAN of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceVlanChanged(IDevice device) 
	{ /* Nothing we need to do, since we're not using VLANs */ }
	
	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId) 
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since we'll get a linkDiscoveryUpdate event */ }

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return this.MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) 
	{ return false; }
	
    /**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{
		Collection<Class<? extends IFloodlightService>> services =
					new ArrayList<Class<? extends IFloodlightService>>();
		services.add(InterfaceShortestPathSwitching.class);
		return services; 
	}

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ 
        Map<Class<? extends IFloodlightService>, IFloodlightService> services =
        			new HashMap<Class<? extends IFloodlightService>, 
        					IFloodlightService>();
        // We are the class that implements the service
        services.put(InterfaceShortestPathSwitching.class, this);
        return services;
	}

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> modules =
	            new ArrayList<Class<? extends IFloodlightService>>();
		modules.add(IFloodlightProviderService.class);
		modules.add(ILinkDiscoveryService.class);
		modules.add(IDeviceService.class);
        return modules;
	}


	
	

}

