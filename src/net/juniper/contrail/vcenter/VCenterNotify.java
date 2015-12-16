/**
 * Copyright (c) 2014 Juniper Networks, Inc. All rights reserved.
 */
package net.juniper.contrail.vcenter;

import java.net.URL;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim.cf.ManagedObjectWatcher;
import com.vmware.vim25.ArrayOfEvent;
import com.vmware.vim25.ArrayOfGuestNicInfo;
import com.vmware.vim25.Event;
import com.vmware.vim25.EventFilterSpec;
import com.vmware.vim25.EventFilterSpecByEntity;
import com.vmware.vim25.EventFilterSpecRecursionOption;
import com.vmware.vim25.ObjectSpec;
import com.vmware.vim25.ObjectUpdate;
import com.vmware.vim25.ObjectUpdateKind;
import com.vmware.vim25.PropertyChange;
import com.vmware.vim25.PropertyChangeOp;
import com.vmware.vim25.PropertyFilterSpec;
import com.vmware.vim25.PropertyFilterUpdate;
import com.vmware.vim25.PropertySpec;
import com.vmware.vim25.RequestCanceled;
import com.vmware.vim25.SelectionSpec;
import com.vmware.vim25.UpdateSet;
import com.vmware.vim25.VirtualMachineToolsRunningStatus;
import com.vmware.vim25.VmEvent;
import com.vmware.vim25.VmMacChangedEvent;
import com.vmware.vim25.VmPoweredOnEvent;
import com.vmware.vim25.VmReconfiguredEvent;
import com.vmware.vim25.VmPoweredOffEvent;
import com.vmware.vim25.DvsEvent;
import com.vmware.vim25.DVPortgroupEvent;
import com.vmware.vim25.DVPortgroupCreatedEvent;
import com.vmware.vim25.DVPortgroupDestroyedEvent;
import com.vmware.vim25.DVPortgroupReconfiguredEvent;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.EventHistoryCollector;
import com.vmware.vim25.mo.EventManager;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.PropertyCollector;
import com.vmware.vim25.mo.PropertyFilter;
import com.vmware.vim25.mo.ServiceInstance;

import net.juniper.contrail.watchdog.TaskWatchDog;

import com.vmware.vim25.VmMigratedEvent;
import com.vmware.vim25.EnteredMaintenanceModeEvent;
import com.vmware.vim25.ExitMaintenanceModeEvent;
import com.vmware.vim25.GuestNicInfo;
import com.vmware.vim25.HostConnectedEvent;
import com.vmware.vim25.HostConnectionLostEvent;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.RuntimeFault;

import com.google.common.base.Throwables;

import org.apache.log4j.Logger;

/**
 * @author Sachchidanand Vaidya
 *
 */
public class VCenterNotify implements Runnable
{

    private static final Logger s_logger =
            Logger.getLogger(VCenterNotify.class);
    static VCenterMonitorTask monitorTask = null;
    static volatile VCenterDB vcenterDB;
    static volatile VncDB vncDB;
    private static ManagedObjectWatcher mom = null;
    private boolean AddPortSyncAtPluginStart = true;
    private boolean VncDBInitComplete = false;
    private boolean VcenterDBInitComplete = false;
    public boolean VCenterNotifyForceRefresh = false;
    static volatile boolean syncNeeded = true;

    static volatile ConcurrentMap<String, VmwareVirtualMachineInfo> watchedVMs 
            = new ConcurrentHashMap<String, VmwareVirtualMachineInfo>();
    
    private final static String[] guestProps = { "guest.toolsRunningStatus", "guest.net" };

    private static PropertyFilter propFilter;
    private static PropertyCollector propColl;
    private static Boolean shouldRun;
    private static Thread watchUpdates = null;

    private static final String[] handledEvents = {
            // Host events
            "HostConnectionLostEvent",
            "HostConnectedEvent",
            "EnteredMaintenanceModeEvent",
            "ExitMaintenanceModeEvent",

            // VM events
            // VM create events
            "VmBeingCreated",
            "VmCreatedEvent",
            "VmClonedEvent",
            "VmCloneEvent",
            "VmDeployedEvent",
            // VM modify events
            "VmPoweredOnEvent",
            "VmPoweredOffEvent",
            "VmRenamedEvent",
            "VmMacChangedEvent",
            "VmMacAssignedEvent",
            "VmReconfiguredEvent",
            "VmConnectedEvent",
            "VmEmigratingEvent",
            "VmMigratedEvent",
            "VmBeingMigratedEvent",
            "VmBeingHotMigratedEvent",
            // VM delete events
            "VmRemovedEvent",

            // DV Port group events
            // DV port create
            "DVPortgroupCreatedEvent",
            // DV port modify
            "DVPortgroupReconfiguredEvent",
            "DVPortgroupRenamedEvent",
            // DV port delete
            "DVPortgroupDestroyedEvent",

            // General
            "MigrationEvent",
    };

    public VCenterNotify(
            String vcenterUrl, String vcenterUsername,
            String vcenterPassword, String dcName,
            String dvsName, String ipFabricPgName,
            String _apiServerAddress, int _apiServerPort, 
            String _username, String _password,
            String _tenant,
            String _authtype, String _authurl, Mode mode)
    {        
        vcenterDB = new VCenterDB(vcenterUrl, vcenterUsername, vcenterPassword,
                dcName, dvsName, ipFabricPgName, mode);
        
        switch (mode) {
        case VCENTER_ONLY:
            vncDB = new VncDB(_apiServerAddress, _apiServerPort, mode);
            break;
        case VCENTER_AS_COMPUTE:
            vncDB = new VncDB(_apiServerAddress, _apiServerPort, _username, _password,
                    _tenant,
                    _authtype, _authurl, mode);
            break;
        default:
            vncDB = new VncDB(_apiServerAddress, _apiServerPort, mode);
        }
    }

    public static VCenterDB getVcenterDB() {
        return vcenterDB;
    }
    
    public boolean getVCenterNotifyForceRefresh() {
        return VCenterNotifyForceRefresh;
    }

    public void setVCenterNotifyForceRefresh(boolean _VCenterNotifyForceRefresh) {
        VCenterNotifyForceRefresh = _VCenterNotifyForceRefresh;
    }

    public void setAddPortSyncAtPluginStart(boolean _AddPortSyncAtPluginStart)
    {
        AddPortSyncAtPluginStart = _AddPortSyncAtPluginStart;
    }

    public boolean getAddPortSyncAtPluginStart()
    {
        return AddPortSyncAtPluginStart;
    }

    private static void updateFilter() {
        mom.cleanUp();
        if (watchedVMs.size() > 0) {
            ManagedEntity[] mes = new ManagedEntity[watchedVMs.size()];
            int i = 0;
            for (VmwareVirtualMachineInfo vmInfo : watchedVMs.values()) {
                mes[i++] = vmInfo.vm;
            }
            mom.watch(mes, guestProps);
        }
    }
    
    public void Cleanup() {        
        mom.cleanUp();
        mom = null;
    }
 
    public static void addVm(VmwareVirtualMachineInfo vmInfo) {
        if (VCenterMonitor.mode == Mode.VCENTER_AS_COMPUTE) {
            return;
        }
        watchedVMs.put(vmInfo.vm.getMOR().getVal(), vmInfo);
    }

    public static void watchVm(VmwareVirtualMachineInfo vmInfo) {
        if (VCenterMonitor.mode == Mode.VCENTER_AS_COMPUTE) {
            return;
        }
        watchedVMs.put(vmInfo.vm.getMOR().getVal(), vmInfo);
        updateFilter();
    }

    public static void unwatchVm(VmwareVirtualMachineInfo vmInfo) {
        if (VCenterMonitor.mode == Mode.VCENTER_AS_COMPUTE) {
            return;
        }
        watchedVMs.remove(vmInfo.vm.getMOR());
        updateFilter();
    }

    private EventHistoryCollector createEventHistoryCollector() throws Exception
    {
        // Create an Entity Event Filter Spec to
        // specify the MoRef of the VM to be get events filtered for
        EventFilterSpecByEntity entitySpec = new EventFilterSpecByEntity();
        entitySpec.setEntity(vcenterDB.getDatacenter().getMOR());
        entitySpec.setRecursion(EventFilterSpecRecursionOption.children);

        // set the entity spec in the EventFilter
        EventFilterSpec eventFilter = new EventFilterSpec();
        eventFilter.setEntity(entitySpec);

        // we are only interested in getting events for the VM.
        // Add as many events you want to track relating to vm.
        // Refer to API Data Object vmEvent and see the extends class list for
        // elaborate list of vmEvents

        eventFilter.setType(handledEvents);

        // create the EventHistoryCollector to monitor events for a VM
        // and get the ManagedObjectReference of the EventHistoryCollector
        // returned
        return vcenterDB.getServiceInstance().getEventManager()
                .createCollectorForEvents(eventFilter);
    }

    private PropertyFilterSpec createEventFilterSpec() throws Exception
    {
        EventHistoryCollector eventHistoryCollector = createEventHistoryCollector();
        // Set up a PropertySpec to use the latestPage attribute
        // of the EventHistoryCollector

        PropertySpec propSpec = new PropertySpec();
        propSpec.setAll(new Boolean(false));
        propSpec.setPathSet(new String[] { "latestPage" });
        propSpec.setType(eventHistoryCollector.getMOR().getType());

        // PropertySpecs are wrapped in a PropertySpec array
        PropertySpec[] propSpecAry = new PropertySpec[] { propSpec };

        // Set up an ObjectSpec with the above PropertySpec for the
        // EventHistoryCollector we just created
        // as the Root or Starting Object to get Attributes for.
        ObjectSpec objSpec = new ObjectSpec();
        objSpec.setObj(eventHistoryCollector.getMOR());
        objSpec.setSkip(new Boolean(false));

        // Get Event objects in "latestPage" from "EventHistoryCollector"
        // and no "traversal" further, so, no SelectionSpec is specified
        objSpec.setSelectSet(new SelectionSpec[] {});

        // ObjectSpecs are wrapped in an ObjectSpec array
        ObjectSpec[] objSpecAry = new ObjectSpec[] { objSpec };

        PropertyFilterSpec spec = new PropertyFilterSpec();
        spec.setPropSet(propSpecAry);
        spec.setObjectSet(objSpecAry);
        return spec;
    }

    private void handleUpdate(UpdateSet update) throws Exception
    {
        ObjectUpdate[] vmUpdates;
        PropertyFilterUpdate[] pfus = update.getFilterSet();
        for (int pfui = 0; pfui < pfus.length; pfui++)
        {
            vmUpdates = pfus[pfui].getObjectSet();
            
            for (ObjectUpdate vmi : vmUpdates)
            {
                handleChanges(vmi);
            }
        }
    }

    void handleChanges(ObjectUpdate oUpdate) throws Exception
    {
        s_logger.info("+++++++++++++Received vcenter update of type " 
                + oUpdate.getKind() + "+++++++++++++");
        
        PropertyChange[] changes = oUpdate.getChangeSet();
        if (changes == null) {
            s_logger.error("handleChanges received null change array from vCenter");
            return;
        }

        String toolsRunningStatus = null;
        GuestNicInfo[] nics = null;        
        for (int pci = 0; pci < changes.length; ++pci)
        {
            if (changes[pci] == null) {
                s_logger.error("handleChanges received null change value from vCenter");
                continue;
            }
            Object value = changes[pci].getVal();
            if (value == null) {
                s_logger.error("handleChanges received null change value from vCenter");
                continue;
            }
            
            PropertyChangeOp op = changes[pci].getOp();
            if (op!= PropertyChangeOp.remove) {
                if (value instanceof ArrayOfEvent) {
                    ArrayOfEvent aoe = (ArrayOfEvent) value;
                    Event[] evts = aoe.getEvent();
                    if (evts == null) {
                        s_logger.error("handleChanges received null event array from vCenter");
                        continue;
                    }
                    for (int evtID = 0; evtID < evts.length; ++evtID)
                    {
                        Event anEvent = evts[evtID];
                        if (anEvent == null) {
                            s_logger.error("handleChanges received null event from vCenter");
                            continue;
                        }
                        s_logger.info("\n----------" + "\n Event ID: "
                                + anEvent.getKey() + "\n Event: "
                                + anEvent.getClass().getName()
                                + "\n FullFormattedMessage: "
                                + anEvent.getFullFormattedMessage()
                                + "\n----------\n");
                    }
                } else if ((value instanceof EnteredMaintenanceModeEvent) || (value instanceof HostConnectionLostEvent)) {
                    Event anEvent = (Event) value;
                    String vRouterIpAddress = vcenterDB.esxiToVRouterIpMap.get(anEvent.getHost().getName());
                    if (vRouterIpAddress != null) {
                        vcenterDB.vRouterActiveMap.put(vRouterIpAddress, false);
                        s_logger.info("\nEntering maintenance mode. Marking the host " + vRouterIpAddress +" inactive");
                    } else {
                        s_logger.info("\nNot managing the host " + vRouterIpAddress +" inactive");
                    }
                } else if ((value instanceof ExitMaintenanceModeEvent) || (value instanceof HostConnectedEvent)) {
                    Event anEvent = (Event) value;
                    String vRouterIpAddress = vcenterDB.esxiToVRouterIpMap.get(anEvent.getHost().getName());
                    if (vRouterIpAddress != null) {
                        vcenterDB.vRouterActiveMap.put(vRouterIpAddress, true);
                        s_logger.info("\nExit maintenance mode. Marking the host " + vRouterIpAddress +" active");
                    } else {
                        s_logger.info("\nNot managing the host " + vRouterIpAddress +" inactive");
                    }
                } else if (value instanceof ArrayOfGuestNicInfo) {
                    s_logger.info("Received update array of GuestNics");
                    ArrayOfGuestNicInfo aog = (ArrayOfGuestNicInfo) value;
                    nics = aog.getGuestNicInfo();
                    
                } else if (value instanceof String) {
                    String propName = changes[pci].getName();
                    String sValue = (String)value;
                   
                    if (propName.equals("guest.toolsRunningStatus")) {
                        toolsRunningStatus = sValue;
                    } else {
                        s_logger.warn("\n Unhandled property change " + propName + " with value " + sValue);
                    }
                } else if (value instanceof Event) {
                    VCenterEventHandler handler = new VCenterEventHandler(
                            (Event) value, vcenterDB, vncDB);
                    // for now we handle the events in the same thread
                    if (vncDB.isVncApiServerAlive()) {
                        handler.handle();
                    } else {
                        syncNeeded = true;
                    }
                } else {
                    s_logger.info("\n Received unhandled property of type " + value.getClass().getName());
                }
            } else if (op == PropertyChangeOp.remove) {

            }
        }

        if (toolsRunningStatus != null || nics != null) {
            ManagedObjectReference mor = oUpdate.getObj();
            if (watchedVMs.containsKey(mor.getVal())) {
                VmwareVirtualMachineInfo vmInfo = watchedVMs.get(mor.getVal());
                if (toolsRunningStatus != null) {
                    vmInfo.setToolsRunningStatus(toolsRunningStatus);
                }
                if (vmInfo.getToolsRunningStatus().equals(VirtualMachineToolsRunningStatus.guestToolsRunning.toString())
                        && nics != null) {
                    vmInfo.updatedGuestNics(nics,vncDB);
                }
            }
        }
        s_logger.info("+++++++++++++Update Processing Complete +++++++++++++++++++++");
    }

    public void start() {
        try
        {
            watchUpdates = new Thread(this);
            shouldRun = true;
            watchUpdates.start();
        } catch (Exception e)
        {
            System.out.println("Caught Exception : " + " Name : "
                    + e.getClass().getName() + " Message : " + e.getMessage()
                    + " Trace : ");
            e.printStackTrace();
        }
    }

    public void terminate() throws Exception {
        shouldRun = false;
        propColl.cancelWaitForUpdates();
        propFilter.destroyPropertyFilter();
        vcenterDB.getServiceInstance().getServerConnection().logout();
        watchUpdates.stop();
    }

    private void connect2vnc() {
        TaskWatchDog.startMonitoring(this, "Init Vnc", 
                300000, TimeUnit.MILLISECONDS);   
        try {
            if (vncDB.Initialize() == true) {
                VncDBInitComplete = true;
            }
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error("Error while initializing Vnc connection: " + e); 
            s_logger.error(stackTrace); 
            e.printStackTrace();
        }
        TaskWatchDog.stopMonitoring(this);
    }

    private void connect2vcenter() {       
        TaskWatchDog.startMonitoring(this, "Init VCenter", 
                300000, TimeUnit.MILLISECONDS);
        try {
            if (vcenterDB.connect() == true) {
                
                PropertyFilterSpec eventFilterSpec = createEventFilterSpec();
                propColl = vcenterDB.getServiceInstance().getPropertyCollector();

                propFilter = propColl.createFilter(eventFilterSpec, true);

                mom = new ManagedObjectWatcher(vcenterDB.getServiceInstance().getPropertyCollector());
                updateFilter();
                
                VcenterDBInitComplete = true;
            }
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error("Error while initializing VCenter connection: " + e); 
            s_logger.error(stackTrace); 
            e.printStackTrace();
        }
        TaskWatchDog.stopMonitoring(this);
    }

    @Override
    public void run()
    {
        String version = "";
        try
        {
            do
            {
                //check if you are the master from time to time
                //sometimes things dont go as planned
                if (VCenterMonitor.isZookeeperLeader() == false) {
                    s_logger.debug("Lost zookeeper leadership. Restarting myself\n");
                    System.exit(0);
                }

                if (VncDBInitComplete == false) {
                    connect2vnc();
                }
                if (VcenterDBInitComplete == false) {
                    connect2vcenter();
                }

                // Perform sync between VNC and VCenter DBs.
                if (getAddPortSyncAtPluginStart() == true || syncNeeded) {
                    TaskWatchDog.startMonitoring(this, "Sync",
                            300000, TimeUnit.MILLISECONDS);

                    // When syncVirtualNetworks is run the first time, it also does
                    // addPort to vrouter agent for existing VMIs.
                    // Clear the flag  on first run of syncVirtualNetworks.
                    try {
                        vcenterDB.setReadTimeout(VCenterDB.VCENTER_READ_TIMEOUT);
                        MainDB.sync(vcenterDB, vncDB, VCenterMonitor.mode);
                        vcenterDB.setReadTimeout(0);
                        syncNeeded = false;
                        setAddPortSyncAtPluginStart(false);
                    } catch (Exception e) {
                        String stackTrace = Throwables.getStackTraceAsString(e);
                        s_logger.error("Error in sync: " + e); 
                        s_logger.error(stackTrace);
                        e.printStackTrace();
                        if (stackTrace.contains("java.net.ConnectException: Connection refused") ||
                            stackTrace.contains("java.rmi.RemoteException: VI SDK invoke"))   {
                                //Remote Exception. Some issue with connection to vcenter-server
                                // Exception on accessing remote objects.
                                // Try to reinitialize the VCenter connection.
                                //For some reason RemoteException not thrown
                                s_logger.error("Problem with connection to vCenter-Server");
                                s_logger.error("Restart connection and reSync");
                                connect2vcenter();
                                version = "";
                        }
                    }
                    TaskWatchDog.stopMonitoring(this);
                }

                try
                {
                    UpdateSet update = propColl.waitForUpdates(version);
                    if (update != null && update.getFilterSet() != null)
                    {

                        version = update.getVersion();

                        this.handleUpdate(update);

                    } else
                    {
                        s_logger.error("No update is present!");
                    }
                } catch (Exception e)
                {
                    syncNeeded = true;
                    s_logger.error("Error in event handling, resync needed");
                    String stackTrace = Throwables.getStackTraceAsString(e);
                    s_logger.error(stackTrace);
                    e.printStackTrace();
                    if (stackTrace.contains("java.net.ConnectException: Connection refused") ||
                        stackTrace.contains("java.rmi.RemoteException: VI SDK invoke"))   {
                            //Remote Exception. Some issue with connection to vcenter-server
                            // Exception on accessing remote objects.
                            // Try to reinitialize the VCenter connection.
                            //For some reason RemoteException not thrown
                            s_logger.error("Problem with connection to vCenter-Server");
                            s_logger.error("Restart connection and reSync");
                            connect2vcenter();
                            version = "";
                    }
                }
            } while (shouldRun);
        } catch (Exception e)
        {
            if (e instanceof RequestCanceled)
            {
                System.out.println("OK");
            } else
            {
                s_logger.error("Caught Exception : " + " Name : "
                        + e.getClass().getName() + " Message : "
                        + e.getMessage() + " Trace : ");
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
            }
        }
    }

    void printVmEvent(Object value)
    {
        VmEvent anEvent = (VmEvent) value;
        s_logger.info("\n----------" + "\n Event ID: "
                + anEvent.getKey() + "\n Event: "
                + anEvent.getClass().getName()
                + "\n FullFormattedMessage: "
                + anEvent.getFullFormattedMessage()
                + "\n VM Reference: "
                + anEvent.getVm().getVm().get_value()
                + "\n createdTime : "
                + anEvent.getCreatedTime().getTime()
                + "\n----------\n");
    }

    public static void stopUpdates() {
        propColl.stopUpdates();
    }
    
    public static VncDB getVncDB() {
        return vncDB;
    }
}

