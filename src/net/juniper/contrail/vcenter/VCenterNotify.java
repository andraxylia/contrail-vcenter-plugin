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
    volatile VCenterDB vcenterDB;
    volatile VncDB vncDB;
    private final String contrailDataCenterName;
    private final String vcenterUrl;
    private final String vcenterUsername;
    private final String vcenterPassword;
    volatile static ServiceInstance serviceInstance;
    private Folder rootFolder;
    private InventoryNavigator inventoryNavigator;
    private Datacenter _contrailDC;
    private static ManagedObjectWatcher mom = null;

    static volatile ConcurrentMap<String, VmwareVirtualMachineInfo> watchedVMs 
            = new ConcurrentHashMap<String, VmwareVirtualMachineInfo>();
    
    private final static String[] guestProps = { "guest.toolsRunningStatus", "guest.net" };

    // EventManager and EventHistoryCollector References
    private EventManager _eventManager;
    private EventHistoryCollector _eventHistoryCollector;
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

            // DVS port events
            // DVS port create
            "DvsPortCreatedEvent",
            // DVS port modify
            "DvsPortJoinPortgroupEvent",
            "DvsPortLeavePortgroupEvent",
            // DVS port deleted
            "DvsPortDeletedEvent",

            // General
            "MigrationEvent",
    };

    public VCenterNotify(VCenterMonitorTask _monitorTask, VCenterDB vcenterDB,
            VncDB vncDB,
            String vcenterUrl, String vcenterUsername,
            String vcenterPassword, String contrailDcName)
    {
        this.monitorTask            = _monitorTask;
        this.vcenterDB              = vcenterDB;
        this.vncDB                  = vncDB;
        this.vcenterUrl             = vcenterUrl;
        this.vcenterUsername        = vcenterUsername;
        this.vcenterPassword        = vcenterPassword;
        this.contrailDataCenterName = contrailDcName;
    }

    /**
     * Initialize the necessary Managed Object References needed here
     */
    private boolean initialize() {
        // Connect to VCenter
        s_logger.info("Connecting to vCenter Server : " + "("
                + vcenterUrl + "," + vcenterUsername + ")");
        if (serviceInstance == null) {
            try {
                serviceInstance = new ServiceInstance(new URL(vcenterUrl),
                        vcenterUsername, vcenterPassword, true);
                if (serviceInstance == null) {
                    s_logger.error("Failed to connect to vCenter Server : " + "("
                            + vcenterUrl + "," + vcenterUsername + ","
                            + vcenterPassword + ")");
                    return false;
                }
            } catch (MalformedURLException e) {
                return false;
            } catch (RemoteException e) {
                s_logger.error("Remote exception while connecting to vcenter" + e);
                e.printStackTrace();
                return false;
            } catch (Exception e) {
                s_logger.error("Error while connecting to vcenter" + e);
                e.printStackTrace();
                return false;
            }
        }
        s_logger.info("Connected to vCenter Server : " + "("
                + vcenterUrl + "," + vcenterUsername + ","
                + vcenterPassword + ")");

        if (rootFolder == null) {
            rootFolder = serviceInstance.getRootFolder();
            if (rootFolder == null) {
                s_logger.error("Failed to get rootfolder for vCenter ");
                return false;
            }
        }
        s_logger.error("Got rootfolder for vCenter ");

        if (inventoryNavigator == null) {
            inventoryNavigator = new InventoryNavigator(rootFolder);
            if (inventoryNavigator == null) {
                s_logger.error("Failed to get InventoryNavigator for vCenter ");
                return false;
            }
        }
        s_logger.error("Got InventoryNavigator for vCenter ");

        // Search contrailDc
        if (_contrailDC == null) {
            try {
                _contrailDC = (Datacenter) inventoryNavigator.searchManagedEntity(
                        "Datacenter", contrailDataCenterName);
            } catch (InvalidProperty e) {
                return false;
            } catch (RuntimeFault e) {
                return false;
            } catch (RemoteException e) {
                return false;
            }
            if (_contrailDC == null) {
                s_logger.error("Failed to find " + contrailDataCenterName
                        + " DC on vCenter ");
                return false;
            }
        }
        s_logger.info("Found " + contrailDataCenterName + " DC on vCenter ");
        if (_eventManager == null) {
            _eventManager = serviceInstance.getEventManager();
        }
        
        mom = new ManagedObjectWatcher(serviceInstance.getPropertyCollector());
        updateFilter();
                
        return true;
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
        serviceInstance    = null;
        rootFolder         = null;
        inventoryNavigator = null;
        _contrailDC        = null;
        _eventManager      = null;
        
        mom.cleanUp();
        mom = null;
    }
    
    public static void addVm(VmwareVirtualMachineInfo vmInfo) {
        watchedVMs.put(vmInfo.vm.getMOR().getVal(), vmInfo);
    }

    public static void watchVm(VmwareVirtualMachineInfo vmInfo) {
        watchedVMs.put(vmInfo.vm.getMOR().getVal(), vmInfo);
        updateFilter();
    }

    public static void unwatchVm(VmwareVirtualMachineInfo vmInfo) {
        watchedVMs.remove(vmInfo.vm.getMOR());
        updateFilter();
    }

    private void createEventHistoryCollector() throws Exception
    {
        // Create an Entity Event Filter Spec to
        // specify the MoRef of the VM to be get events filtered for
        EventFilterSpecByEntity entitySpec = new EventFilterSpecByEntity();
        entitySpec.setEntity(_contrailDC.getMOR());
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
        _eventHistoryCollector = _eventManager
                .createCollectorForEvents(eventFilter);
    }

    private PropertyFilterSpec createEventFilterSpec()
    {
        // Set up a PropertySpec to use the latestPage attribute
        // of the EventHistoryCollector

        PropertySpec propSpec = new PropertySpec();
        propSpec.setAll(new Boolean(false));
        propSpec.setPathSet(new String[] { "latestPage" });
        propSpec.setType(_eventHistoryCollector.getMOR().getType());

        // PropertySpecs are wrapped in a PropertySpec array
        PropertySpec[] propSpecAry = new PropertySpec[] { propSpec };

        // Set up an ObjectSpec with the above PropertySpec for the
        // EventHistoryCollector we just created
        // as the Root or Starting Object to get Attributes for.
        ObjectSpec objSpec = new ObjectSpec();
        objSpec.setObj(_eventHistoryCollector.getMOR());
        objSpec.setSkip(new Boolean(false));

        // Get Event objects in "latestPage" from "EventHistoryCollector"
        // and no "traversl" further, so, no SelectionSpec is specified
        objSpec.setSelectSet(new SelectionSpec[] {});

        // ObjectSpecs are wrapped in an ObjectSpec array
        ObjectSpec[] objSpecAry = new ObjectSpec[] { objSpec };

        PropertyFilterSpec spec = new PropertyFilterSpec();
        spec.setPropSet(propSpecAry);
        spec.setObjectSet(objSpecAry);
        return spec;
    }

    private void handleUpdate(UpdateSet update)
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

    void handleChanges(ObjectUpdate oUpdate)
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
                    handler.run();
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
                    try {
                    vmInfo.updatedGuestNics(nics,vncDB);
                    } catch (Exception e) {
                        // log unable to process event;
                        // this triggers a sync
                        s_logger.info("Exception received, resync triggered" + e.getMessage());
                        VCenterMonitorTask.syncNeeded = true;
                    }
                }
            }
        }
        s_logger.info("+++++++++++++Update Processing Complete +++++++++++++++++++++");
    }

    public void start() {
        try
        {
            this.initialize();
            System.out.println("info---" +
                    serviceInstance.getAboutInfo().getFullName());
            this.createEventHistoryCollector();

            PropertyFilterSpec eventFilterSpec = this
                    .createEventFilterSpec();
            propColl = serviceInstance.getPropertyCollector();

            propFilter = propColl.createFilter(eventFilterSpec, true);

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

    public static void terminate() throws Exception {
        shouldRun = false;
        propColl.cancelWaitForUpdates();
        propFilter.destroyPropertyFilter();
        serviceInstance.getServerConnection().logout();
        watchUpdates.stop();
    }

    @Override
    public void run()
    {
        String version = "";
        try
        {
            do
            {
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
                    String stackTrace = Throwables.getStackTraceAsString(e);
                    s_logger.error(stackTrace);
                    s_logger.error("Exception in ServiceInstance. Refreshing the serviceinstance and starting new");
                    do {
                        System.out.println("Waiting for periodic thread to reconnect...");
                        Thread.sleep(2000);
                        if (monitorTask.getVCenterNotifyForceRefresh()) {
                            s_logger.info("periodic thread reconnect successful.. initialize Notify..");
                            Cleanup();
                            initialize();
                            createEventHistoryCollector();
                            PropertyFilterSpec eventFilterSpec = createEventFilterSpec();
                            propColl = serviceInstance.getPropertyCollector();
                            propFilter = propColl.createFilter(eventFilterSpec, true);
                            monitorTask.setVCenterNotifyForceRefresh(false);
                            version = "";
                            s_logger.info("reInit Notify Complete..");
                            break;
                        }
                    } while (true);
                    continue;
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

    void printDvsPortgroupEvent(Object value)
    {
        DVPortgroupEvent anEvent = (DVPortgroupEvent) value;
        s_logger.info("\n----------" + "\n Event ID: "
                + anEvent.getKey() + "\n Event: "
                + anEvent.getClass().getName()
                + "\n FullFormattedMessage: "
                + anEvent.getFullFormattedMessage()
                + "\n DVS Portgroup Reference: "
                + anEvent.getDvs().getDvs().get_value()
                + "\n----------\n");
    }

    public static void stopUpdates() {
        propColl.stopUpdates();
    }
}

