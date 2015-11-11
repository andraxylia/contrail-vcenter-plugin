/**
 * Copyright (c) 2014 Juniper Networks, Inc. All rights reserved.
 */

package net.juniper.contrail.vcenter;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.io.FileNotFoundException;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.VirtualMachineToolsRunningStatus;
import org.apache.log4j.Logger;
import com.vmware.vim25.DVPortSetting;
import com.vmware.vim25.DVPortgroupConfigInfo;
import com.vmware.vim25.DVSConfigInfo;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.GuestNicInfo;
import com.vmware.vim25.IpPool;
import com.vmware.vim25.IpPoolIpPoolConfigInfo;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.NetIpConfigInfo;
import com.vmware.vim25.NetIpConfigInfoIpAddress;
import com.vmware.vim25.NetworkSummary;
import com.vmware.vim25.VMwareDVSPortSetting;
import com.vmware.vim25.VirtualDevice;
import com.vmware.vim25.VirtualDeviceBackingInfo;
import com.vmware.vim25.VirtualEthernetCard;
import com.vmware.vim25.VirtualEthernetCardDistributedVirtualPortBackingInfo;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineConfigSpec;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.VmwareDistributedVirtualSwitchPvlanSpec;
import com.vmware.vim25.VmwareDistributedVirtualSwitchVlanIdSpec;
import com.vmware.vim25.VmwareDistributedVirtualSwitchVlanSpec;
import com.vmware.vim25.DistributedVirtualSwitchKeyedOpaqueBlob;
import com.vmware.vim25.VMwareDVSConfigInfo;
import com.vmware.vim25.VMwareDVSPvlanMapEntry;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.DistributedVirtualPortgroup;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.IpPoolManager;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ManagedObject;
import com.vmware.vim25.mo.Network;
import com.vmware.vim25.mo.util.PropertyCollectorUtil;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.VirtualMachine;
import com.vmware.vim25.mo.VmwareDistributedVirtualSwitch;

public class VCenterDB {
    private static final Logger s_logger =
            Logger.getLogger(VCenterDB.class);
    protected static final String contrailVRouterVmNamePrefix = "contrailVM";
    private static final String esxiToVRouterIpMapFile = "/etc/contrail/ESXiToVRouterIp.map";
    private static final int SERVICE_INSTANCE_READ_TIMEOUT = 30000; //30 sec
    protected final String contrailDvSwitchName;
    private final String contrailDataCenterName;
    private final String vcenterUrl;
    private final String vcenterUsername;
    private final String vcenterPassword;
    private final String contrailIpFabricPgName;
    private final String mode;
    
    static volatile ServiceInstance serviceInstance;
    private volatile Folder rootFolder;
    private volatile InventoryNavigator inventoryNavigator;
    private volatile IpPoolManager ipPoolManager;
    protected volatile Datacenter contrailDC;
    protected volatile VmwareDistributedVirtualSwitch contrailDVS;
    private volatile SortedMap<String, VmwareVirtualNetworkInfo> prevVmwareVNInfos;
    private volatile ConcurrentMap<String, Datacenter> datacenters;
    private volatile ConcurrentMap<String, VmwareDistributedVirtualSwitch> dvswitches;

    public volatile Map<String, String> esxiToVRouterIpMap;
    public volatile Map<String, Boolean> vRouterActiveMap;

    public VCenterDB(String vcenterUrl, String vcenterUsername,
                     String vcenterPassword, String contrailDcName,
                     String contrailDvsName, String ipFabricPgName, String mode) {
        this.vcenterUrl             = vcenterUrl;
        this.vcenterUsername        = vcenterUsername;
        this.vcenterPassword        = vcenterPassword;
        this.contrailDataCenterName = contrailDcName;
        this.contrailDvSwitchName   = contrailDvsName;
        this.contrailIpFabricPgName = ipFabricPgName;
        this.mode                   = mode;

        s_logger.info("VCenterDB(" + contrailDvsName + ", " + ipFabricPgName + ")");
        // Create ESXi host to vRouterVM Ip address map
        esxiToVRouterIpMap = new HashMap<String, String>();
        vRouterActiveMap = new HashMap<String, Boolean>();
        datacenters = new ConcurrentHashMap<String, Datacenter>();
        dvswitches = new ConcurrentHashMap<String, VmwareDistributedVirtualSwitch>();
    }

    public boolean Initialize() {

        // Build ESXi to VRouterIp Map
        if (buildEsxiToVRouterIpMap() == false)
            return false;

        s_logger.info("Trying to Connect to vCenter Server : " + "("
                                + vcenterUrl + "," + vcenterUsername + ")");
        // Connect to VCenter
        if (serviceInstance == null) {
            try {
                serviceInstance = new ServiceInstance(new URL(vcenterUrl),
                                            vcenterUsername, vcenterPassword, true);
                if (serviceInstance == null) {
                    s_logger.error("Failed to connect to vCenter Server : " + "("
                                    + vcenterUrl + "," + vcenterUsername + "," 
                                    + vcenterPassword + ")");
                    connectRetry();
                }
            } catch (MalformedURLException e) {
                    return false;
            } catch (RemoteException e) {
               s_logger.error("Remote exception while connecting to vcenter" + e);
                e.printStackTrace();
                return connectRetry();
            } catch (Exception e) {
                s_logger.error("Error while connecting to vcenter" + e);
                e.printStackTrace();
                return false;
            }
        }
        s_logger.info("Connected to vCenter Server : " + "("
                                + vcenterUrl + "," + vcenterUsername + "," 
                                + vcenterPassword + ")");
        //Set read timeout on connection to vcenter server 
        serviceInstance.getServerConnection()
                       .getVimService()
                       .getWsc().setReadTimeout(SERVICE_INSTANCE_READ_TIMEOUT);
        s_logger.info("ServiceInstance read timeout set to " + 
            serviceInstance.getServerConnection().getVimService().getWsc().getReadTimeout());
        return true;
    }

    public boolean Initialize_data() {

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

        if (ipPoolManager == null) {
            ipPoolManager = serviceInstance.getIpPoolManager();
            if (ipPoolManager == null) {
                s_logger.error("Failed to get ipPoolManager for vCenter ");
                return false;
            }
        }
        s_logger.error("Got ipPoolManager for vCenter ");

        // Search contrailDc
        if (contrailDC == null) {
            try {
                contrailDC = (Datacenter) inventoryNavigator.searchManagedEntity(
                                          "Datacenter", contrailDataCenterName);
            } catch (InvalidProperty e) {
                    return false;
            } catch (RuntimeFault e) {
                    return false;
            } catch (RemoteException e) {
                    return false;
            }
            if (contrailDC == null) {
                s_logger.error("Failed to find " + contrailDataCenterName 
                               + " DC on vCenter ");
                return false;
            }
        }
        s_logger.info("Found " + contrailDataCenterName + " DC on vCenter ");
        datacenters.put(contrailDataCenterName, contrailDC);

        // Search contrailDvSwitch
        if (contrailDVS == null) {
            try {
                contrailDVS = (VmwareDistributedVirtualSwitch)
                                inventoryNavigator.searchManagedEntity(
                                        "VmwareDistributedVirtualSwitch",
                                        contrailDvSwitchName);
            } catch (InvalidProperty e) {
                    return false;
            } catch (RuntimeFault e) {
                    return false;
            } catch (RemoteException e) {
                    return false;
            }

            if (contrailDVS == null) {
                s_logger.error("Failed to find " + contrailDvSwitchName + 
                               " DVSwitch on vCenter");
                return false;
            }
        }
        s_logger.info("Found " + contrailDvSwitchName + " DVSwitch on vCenter ");
        dvswitches.put(contrailDvSwitchName, contrailDVS);

        // All well on vCenter front.
        return true;
    }

    public boolean connectRetry() {
        Cleanup();
        while(retryServiceInstance() == false) {
            try{
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }

        }
        s_logger.info("Re-Connect successful!");
        Initialize_data();
        return true;
    }

    public boolean retryServiceInstance() {
        try {
                s_logger.info("Trying to reconnect to vcenter!!");
                serviceInstance = new ServiceInstance(new URL(vcenterUrl),
                                            vcenterUsername, vcenterPassword, true);
                if (serviceInstance == null) {
                    s_logger.error("Failed to connect to vCenter Server : " + "("
                                    + vcenterUrl + "," + vcenterUsername + ","
                                    + vcenterPassword + ")" + "Retrying after 5 secs");
                    return false;
                }
                //Set read timeout on connection to vcenter server
                serviceInstance.getServerConnection()
                       .getVimService()
                       .getWsc().setReadTimeout(SERVICE_INSTANCE_READ_TIMEOUT);
                s_logger.error("ServiceInstance read timeout: " + 
                    serviceInstance.getServerConnection().getVimService().getWsc().getReadTimeout());
                return true;
        } catch (MalformedURLException e) {
                s_logger.info("Re-Connect unsuccessful!");
                return false;
        } catch (RemoteException e) {
                s_logger.info("Re-Connect unsuccessful!");
                return false;
        } catch (Exception e) {
                s_logger.error("Error while connecting to vcenter" + e);
                e.printStackTrace();
                return false;
        }
    }

    public void Cleanup() {
        serviceInstance = null;
        rootFolder = null;
        inventoryNavigator = null;
        ipPoolManager = null;
        contrailDC = null;
        contrailDVS = null;
        VCenterNotify.stopUpdates();
    }


    public boolean buildEsxiToVRouterIpMap() {
        try {
            File file = new File("/etc/contrail/ESXiToVRouterIp.map");
            Scanner input = new Scanner(file);
            while(input.hasNext()) {
                String nextLine = input.nextLine();
                String[] part = nextLine.split(":");
                s_logger.info(" ESXi IP Address:" + part[0] + " vRouter-IP-Address: " + part[1]);
                esxiToVRouterIpMap.put(part[0], part[1]);
                vRouterActiveMap.put(part[1], true);
            }
        } catch (FileNotFoundException e) {
            s_logger.error("file not found :" + esxiToVRouterIpMapFile);
            return false;
        }
        return true;
    }

    public void setPrevVmwareVNInfos(
                    SortedMap<String, VmwareVirtualNetworkInfo> _prevVmwareVNInfos) {
        prevVmwareVNInfos = _prevVmwareVNInfos;
    }

    public SortedMap<String, VmwareVirtualNetworkInfo> getPrevVmwareVNInfos() {
        return prevVmwareVNInfos;
    }

    public ServiceInstance getServiceInstance() {
      return serviceInstance;
    }

    public void setServiceInstance(ServiceInstance _serviceInstance) {
      serviceInstance = _serviceInstance;
    }

    public IpPoolManager getIpPoolManager() {
      return ipPoolManager;
    }

    public void setIpPoolManager(IpPoolManager _ipPoolManager) {
      ipPoolManager = _ipPoolManager;
    }

    public InventoryNavigator getInventoryNavigator() {
      return inventoryNavigator;
    }

    public void setInventoryNavigator(InventoryNavigator _inventoryNavigator) {
      inventoryNavigator = _inventoryNavigator;
    }

    public Datacenter getDatacenter() {
      return contrailDC;
    }

    public void setDatacenter(Datacenter _dc) {
      contrailDC = _dc;
    }

    public IpPool getIpPool(
            DistributedVirtualPortgroup portGroup, IpPool[] ipPools) {

        // If PG to IpPool association exists, check
        NetworkSummary summary = portGroup.getSummary();
        Integer poolid = summary.getIpPoolId();
        if (poolid != null) {
            for (IpPool pool : ipPools) {
                if (pool.id == poolid.intValue()) {
                  return pool;
                }
            }
        }

        // Validate that the IpPool name matches PG names 
        String IpPoolForPG = "ip-pool-for-" + portGroup.getName();
        for (IpPool pool : ipPools) {
            if (IpPoolForPG.equals(pool.getName())) {
                return pool;
            }
        }
        return null;
    }
    
    protected static String getVirtualMachineMacAddress(
            VirtualMachineConfigInfo vmConfigInfo,
            DistributedVirtualPortgroup portGroup) {
        VirtualDevice devices[] = vmConfigInfo.getHardware().getDevice();
        for (VirtualDevice device : devices) {
            // XXX Assuming only one interface
            if (device instanceof VirtualEthernetCard) {
                VirtualDeviceBackingInfo backingInfo = 
                        device.getBacking();

                if (backingInfo == null)
                    continue;

                // Is it backed by the distributed virtual port group? 
                if (backingInfo instanceof 
                    VirtualEthernetCardDistributedVirtualPortBackingInfo) {
                    VirtualEthernetCardDistributedVirtualPortBackingInfo
                    dvpBackingInfo = 
                    (VirtualEthernetCardDistributedVirtualPortBackingInfo)
                    backingInfo;
                    if ((dvpBackingInfo.getPort() == null) ||
                        (dvpBackingInfo.getPort().getPortgroupKey() == null))
                        continue;

                    if (dvpBackingInfo.getPort().getPortgroupKey().
                            equals(portGroup.getKey())) {
                        String vmMac = ((VirtualEthernetCard) device).
                                getMacAddress();
                        return vmMac;
                    }
                }
            } 
        }
        s_logger.error("dvPg: " + portGroup.getName() + " vmConfig: " + 
                vmConfigInfo + " MAC Address NOT found");
        return null;
    }

    protected String getVRouterVMIpFabricAddress(String hostName,
            HostSystem host, String vmNamePrefix)
                    throws Exception {
        // Find if vRouter Ip Fabric mapping exists..
        String vRouterIpAddress = esxiToVRouterIpMap.get(hostName);
        if (host.getRuntime().isInMaintenanceMode())
            vRouterActiveMap.put(vRouterIpAddress, false);
        if (vRouterIpAddress != null) {
            return vRouterIpAddress;
        }

        VirtualMachine[] vms = host.getVms();
        for (VirtualMachine vm : vms) {
            String vmName = vm.getName();
            if (!vmName.toLowerCase().contains(vmNamePrefix.toLowerCase())) {
                continue;
            }
            // XXX Assumption here is that VMware Tools are installed
            // and IP address is available
            GuestInfo guestInfo = vm.getGuest();
            if (guestInfo == null) {
                s_logger.debug(" Host: " + hostName +
                        " vm:" + vmName + " GuestInfo - VMware Tools " +
                        " NOT installed");
                continue;
            }
            GuestNicInfo[] nicInfos = guestInfo.getNet();
            if (nicInfos == null) {
                s_logger.debug(" Host: " + hostName +
                        " vm:" + vmName + " GuestNicInfo - VMware Tools " +
                        " NOT installed");
                continue;
            }
            for (GuestNicInfo nicInfo : nicInfos) {
                // Extract the IP address associated with simple port
                // group. Assumption here is that Contrail VRouter VM will
                // have only one standard port group
                String networkName = nicInfo.getNetwork();
                if (networkName == null || !networkName.equals(contrailIpFabricPgName)) {
                    continue;
                }
                Network network = (Network)
                        inventoryNavigator.searchManagedEntity("Network",
                                networkName);
                if (network == null) {
                    s_logger.debug("Host: " +
                            hostName + " vm: " + vmName + " network: " +
                            networkName + " NOT found");
                    continue;
                }
                NetIpConfigInfo ipConfigInfo = nicInfo.getIpConfig();
                if (ipConfigInfo == null) {
                    continue;
                }
                NetIpConfigInfoIpAddress[] ipAddrConfigInfos =
                        ipConfigInfo.getIpAddress();
                if (ipAddrConfigInfos == null ||
                        ipAddrConfigInfos.length == 0) {
                    continue;

                }
                for (NetIpConfigInfoIpAddress ipAddrConfigInfo :
                    ipAddrConfigInfos) {
                    String ipAddress = ipAddrConfigInfo.getIpAddress();
                    // Choose IPv4 only
                    InetAddress ipAddr = InetAddress.getByName(ipAddress);
                    if (ipAddr instanceof Inet4Address) {
                       // found vRouter VM ip-fabric address. Store it.
                        esxiToVRouterIpMap.put(hostName, ipAddress);
                        return ipAddress;
                    }
                }
            }
        }
        return null;
    }


    private String getVirtualMachineIpAddress(String dvPgName,
            String hostName, HostSystem host, String vmNamePrefix) 
                    throws Exception {
        VirtualMachine[] vms = host.getVms();
        for (VirtualMachine vm : vms) {
            String vmName = vm.getName();
            if (!vmName.toLowerCase().contains(vmNamePrefix.toLowerCase())) {
                continue;
            }
            // XXX Assumption here is that VMware Tools are installed
            // and IP address is available
            GuestInfo guestInfo = vm.getGuest();
            if (guestInfo == null) {
                s_logger.debug("dvPg: " + dvPgName + " host: " + hostName +
                        " vm:" + vmName + " GuestInfo - VMware Tools " +
                        " NOT installed");
                continue;
            }
            GuestNicInfo[] nicInfos = guestInfo.getNet();
            if (nicInfos == null) {
                s_logger.debug("dvPg: " + dvPgName + " host: " + hostName +
                        " vm:" + vmName + " GuestNicInfo - VMware Tools " +
                        " NOT installed");
                continue;
            }
            for (GuestNicInfo nicInfo : nicInfos) {
                // Extract the IP address associated with simple port 
                // group. Assumption here is that Contrail VRouter VM will
                // have only one standard port group
                String networkName = nicInfo.getNetwork();
                if (networkName == null) {
                    continue;
                }
                Network network = (Network)
                        inventoryNavigator.searchManagedEntity("Network",
                                networkName);
                if (network == null) {
                    s_logger.debug("dvPg: " + dvPgName + "host: " + 
                            hostName + " vm: " + vmName + " network: " +
                            networkName + " NOT found");
                    continue;
                }
                if (network instanceof DistributedVirtualPortgroup) {
                    s_logger.debug("dvPg: " + dvPgName + "host: " + 
                            hostName + "vm: " + vmName + " network: " +
                            networkName + " is distributed virtual port " +
                            "group");
                    continue;
                }
                NetIpConfigInfo ipConfigInfo = nicInfo.getIpConfig();
                if (ipConfigInfo == null) {
                    continue;
                }
                NetIpConfigInfoIpAddress[] ipAddrConfigInfos =
                        ipConfigInfo.getIpAddress();
                if (ipAddrConfigInfos == null ||
                        ipAddrConfigInfos.length == 0) {
                    continue;

                }
                for (NetIpConfigInfoIpAddress ipAddrConfigInfo :
                    ipAddrConfigInfos) {
                    String ipAddress = ipAddrConfigInfo.getIpAddress();
                    // Choose IPv4 only
                    InetAddress ipAddr = InetAddress.getByName(ipAddress);
                    if (ipAddr instanceof Inet4Address) {
                        return ipAddress;
                    }
                }

            }
        }
        return null;
    }

    String getVirtualMachineIpAddress(VirtualMachine vm, String dvPgName)
                    throws Exception {

        // Assumption here is that VMware Tools are installed
        // and IP address is available
        GuestInfo guestInfo = vm.getGuest();
        String vmName = vm.getName();
        if (guestInfo == null) {
            s_logger.debug("dvPg: " + dvPgName + " vm:" + vmName
                    + " GuestInfo - VMware Tools " + " NOT installed");
            return null;
        }
        GuestNicInfo[] nicInfos = guestInfo.getNet();
        if (nicInfos == null) {
            s_logger.debug("dvPg: " + dvPgName + " vm:" + vmName
                    + " GuestNicInfo - VMware Tools " + " NOT installed");
            return null;
        }
        for (GuestNicInfo nicInfo : nicInfos) {
            // Extract the IP address associated with simple port
            // group. Assumption here is that Contrail VRouter VM will
            // have only one standard port group
            String networkName = nicInfo.getNetwork();
            if (networkName == null) {
                continue;
            }

            if (!networkName.equals(dvPgName)) {
                continue;
            }

            Network network = (Network) 
                    inventoryNavigator.searchManagedEntity("Network",
                            networkName);
            if (network == null) {
                s_logger.debug("dvPg: " + dvPgName
                        + " vm: " + vmName + " network: " + 
                        networkName + " NOT found");
                continue;
            }
            if (network instanceof DistributedVirtualPortgroup) {
                NetIpConfigInfo ipConfigInfo = nicInfo.getIpConfig();
                if (ipConfigInfo == null) {
                    continue;
                }
                NetIpConfigInfoIpAddress[] ipAddrConfigInfos =
                        ipConfigInfo.getIpAddress();
                if (ipAddrConfigInfos == null ||
                        ipAddrConfigInfos.length == 0) {
                    continue;

                }
                for (NetIpConfigInfoIpAddress ipAddrConfigInfo :
                    ipAddrConfigInfos) {
                    String ipAddress = ipAddrConfigInfo.getIpAddress();
                    // Choose IPv4 only
                    InetAddress ipAddr = InetAddress.getByName(ipAddress);
                    if (ipAddr instanceof Inet4Address) {
                        return ipAddress;
                    }
                }
            }
        }
        return null;
    }
    

    public boolean doIgnoreVirtualMachine(String vmName) {
        // Ignore contrailVRouterVMs since those should not be reflected in
        // Contrail VNC
        if (vmName.toLowerCase().contains(
                contrailVRouterVmNamePrefix.toLowerCase())) {
            return true;
        }
        return false;
    }
    
    VmwareVirtualMachineInfo fillVmwareVirtualMachineInfo(
                                       VirtualMachine vcenterVm,
                                       VirtualMachineConfigInfo vmConfigInfo,
                                       DistributedVirtualPortgroup portGroup)
                                       throws Exception {
        // Name
        String vmName = vcenterVm.getName();
        String dvPgName = portGroup.getName();

        // Ignore virtual machine?
        if (doIgnoreVirtualMachine(vmName)) {
            s_logger.debug("dvPg: " + dvPgName +
                    " Ignoring vm: " + vmName);
            return null;
        }

        // Is it powered on?
        VirtualMachineRuntimeInfo vmRuntimeInfo = vcenterVm.getRuntime();
        VirtualMachinePowerState powerState =
                vmRuntimeInfo.getPowerState();
            s_logger.debug("dvPg: " + dvPgName + " VM: " +
                    vmName + " Power State: " + powerState);

        // Extract MAC address
        String vmMac = getVirtualMachineMacAddress(vmConfigInfo,
                portGroup);
        if (vmMac == null) {
            s_logger.error("dvPg: " + dvPgName + " vm: " +
                    vmName + " MAC Address NOT found");
            return null;
        }

        // Get host information
        ManagedObjectReference hmor = vmRuntimeInfo.getHost();
        HostSystem host = new HostSystem(
            vcenterVm.getServerConnection(), hmor);
        String hostName = host.getName();

        // Get Contrail VRouter virtual machine information from the host
        String vrouterIpAddress = getVRouterVMIpFabricAddress(
                hostName, host, contrailVRouterVmNamePrefix);
        if (vrouterIpAddress == null) {
            s_logger.error("ContrailVM not found on ESXi host: " 
                    + hostName + ", skipping VM (" + vmName + ") creation"
                    + " on network: " + dvPgName);
            return null;
        }

        // found valid vm instance.
        VmwareVirtualMachineInfo vmInfo = new
                VmwareVirtualMachineInfo(vmName, hostName, hmor,
                        vrouterIpAddress, vmMac, powerState);

        return vmInfo;
    }

    private SortedMap<String, VmwareVirtualMachineInfo>
        populateVirtualMachineInfo(
                DistributedVirtualPortgroup portGroup,
                boolean externalIpam) throws Exception {
        String dvPgName = portGroup.getName();
        // Get list of virtual machines connected to the port group
        VirtualMachine[] vms = portGroup.getVms();
        if (vms == null || vms.length == 0) {
            s_logger.debug("dvPg: " + dvPgName +
                    " NO virtual machines connected");
            return null;
        }
        SortedMap<String, VmwareVirtualMachineInfo> vmInfos = 
                new TreeMap<String, VmwareVirtualMachineInfo>();
        for (VirtualMachine vm : vms) {
            // Extract configuration info and get instance UUID
            VirtualMachineConfigInfo vmConfigInfo = vm.getConfig();
            String instanceUuid = vmConfigInfo.getInstanceUuid();

            VmwareVirtualMachineInfo vmInfo = fillVmwareVirtualMachineInfo(vm, vmConfigInfo,
                                                                           portGroup);
            if (vmInfo == null) {
                continue;
            }
            if ((externalIpam == true) && (vmInfo.isPoweredOnState())) {
                String ipAddress = getVirtualMachineIpAddress(vm, dvPgName);
                if (ipAddress != null) {
                  // Ensure that ip-address is within subnet range
                }
                vmInfo.setIpAddress(ipAddress);
            }
            vmInfos.put(instanceUuid, vmInfo);
        }
        if (vmInfos.size() == 0) {
            return null;
        }
        return vmInfos;
    }

    protected static boolean doIgnoreVirtualNetwork(DVPortSetting portSetting) {
        // Ignore dvPgs that do not have PVLAN/VLAN configured
        if (portSetting instanceof VMwareDVSPortSetting) {
            VMwareDVSPortSetting vPortSetting = 
                    (VMwareDVSPortSetting) portSetting;
            VmwareDistributedVirtualSwitchVlanSpec vlanSpec = 
                    vPortSetting.getVlan();
            if (vlanSpec instanceof VmwareDistributedVirtualSwitchPvlanSpec) {
                return false;
            }
            if (vlanSpec instanceof VmwareDistributedVirtualSwitchVlanIdSpec) {
                return false;
            }
        }
        return true;
    }

    public HashMap<String, Short> getVlanInfo(DistributedVirtualPortgroup dvPg,
        DVPortSetting portSetting,
        VMwareDVSPvlanMapEntry[] pvlanMapArray) {

        // Create HashMap which will store private vlan info
        HashMap<String, Short> vlan = new HashMap<String, Short>();

        if (portSetting instanceof VMwareDVSPortSetting) {
            VMwareDVSPortSetting vPortSetting = 
                    (VMwareDVSPortSetting) portSetting;
            VmwareDistributedVirtualSwitchVlanSpec vlanSpec = 
                    vPortSetting.getVlan();
            if (vlanSpec instanceof VmwareDistributedVirtualSwitchPvlanSpec) {
                // Find isolated secondary VLAN Id
                VmwareDistributedVirtualSwitchPvlanSpec pvlanSpec = 
                        (VmwareDistributedVirtualSwitchPvlanSpec) vlanSpec;
                short isolatedVlanId = (short)pvlanSpec.getPvlanId();
                // Find primaryVLAN corresponding to isolated secondary VLAN
                short primaryVlanId = 0;
                for (short i=0; i < pvlanMapArray.length; i++) {
                    if ((short)pvlanMapArray[i].getSecondaryVlanId() != isolatedVlanId)
                        continue;
                    if (!pvlanMapArray[i].getPvlanType().equals("isolated"))
                        continue;
                    s_logger.debug("    VlanType = PrivateVLAN"
                                  + " PrimaryVLAN = " + pvlanMapArray[i].getPrimaryVlanId() 
                                  + " IsolatedVLAN = " + pvlanMapArray[i].getSecondaryVlanId());
                    primaryVlanId = (short)pvlanMapArray[i].getPrimaryVlanId();
                }
                vlan.put("primary-vlan", primaryVlanId);
                vlan.put("secondary-vlan", isolatedVlanId);
            } else if (vlanSpec instanceof VmwareDistributedVirtualSwitchVlanIdSpec) {
                VmwareDistributedVirtualSwitchVlanIdSpec vlanIdSpec = 
                        (VmwareDistributedVirtualSwitchVlanIdSpec) vlanSpec;
                short vlanId = (short)vlanIdSpec.getVlanId();
                s_logger.debug("    VlanType = VLAN " + " VlanId = " + vlanId);
                vlan.put("primary-vlan", vlanId);
                vlan.put("secondary-vlan", vlanId);
            } else {
                s_logger.error("dvPg: " + dvPg.getName() + 
                        " port setting: " +  vPortSetting + 
                        ": INVALID vlan spec: " + vlanSpec);
                return null;
            }
        }

        return vlan;
    }

    public boolean getExternalIpamInfo(DVPortgroupConfigInfo configInfo, String vnName) throws Exception {

        DistributedVirtualSwitchKeyedOpaqueBlob[] opaqueBlobs
                             = configInfo.getVendorSpecificConfig();

        if ((opaqueBlobs == null) || (opaqueBlobs.length == 0)) {
            return false;
        }

        for (DistributedVirtualSwitchKeyedOpaqueBlob opaqueBlob : opaqueBlobs) {
          s_logger.debug("pg (" + vnName + ") " + "opaqueBlob: key ("
                       + opaqueBlob.getKey() + " )  opaqueData ("
                       + opaqueBlob.getOpaqueData() + ")");
          if (opaqueBlob.getKey().equals("external_ipam")) {
              if (opaqueBlob.getOpaqueData().equals("true")) {
                return true;
              } else {
                return false;
              }
          }
        }
        return false;
    }

    public boolean getExternalIpamInfo(DistributedVirtualSwitchKeyedOpaqueBlob[] opaqueBlobs, String vnName) {

        if ((opaqueBlobs == null) || (opaqueBlobs.length == 0)) {
            return false;
        }

        for (DistributedVirtualSwitchKeyedOpaqueBlob opaqueBlob : opaqueBlobs) {
          s_logger.debug("pg (" + vnName + ") " + "opaqueBlob: key ("
                       + opaqueBlob.getKey() + " )  opaqueData ("
                       + opaqueBlob.getOpaqueData() + ")");
          if (opaqueBlob.getKey().equals("external_ipam")) {
              if (opaqueBlob.getOpaqueData().equals("true")) {
                return true;
              } else {
                return false;
              }
          }
        }
        return false;
    }
    
    public SortedMap<String, VmwareVirtualNetworkInfo> 
        populateVirtualNetworkInfo() throws Exception {

        // Search contrailDvSwitch
        //VmwareDistributedVirtualSwitch contrailDvs =
        //        (VmwareDistributedVirtualSwitch)
        //        inventoryNavigator.searchManagedEntity(
        //                "VmwareDistributedVirtualSwitch",
        //                contrailDvSwitchName);
        if (contrailDVS == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName + 
                    " NOT configured");
            return null;
        }
        // Extract distributed virtual port groups 
        DistributedVirtualPortgroup[] dvPgs = contrailDVS.getPortgroup();
        if (dvPgs == null || dvPgs.length == 0) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName + 
                    " Distributed portgroups NOT configured");
            return null;
        }
        // Extract IP Pools
        //Datacenter contrailDC = (Datacenter) inventoryNavigator.
        //        searchManagedEntity(
        //        "Datacenter",
        //        contrailDataCenterName);
        IpPool[] ipPools = ipPoolManager.queryIpPools(contrailDC);
        if (ipPools == null || ipPools.length == 0) {
            s_logger.debug("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " IP Pools NOT " +
                    "configured");
            return null;
        }

        // Extract private vlan entries for the virtual switch
        VMwareDVSConfigInfo dvsConfigInfo = (VMwareDVSConfigInfo) contrailDVS.getConfig();
        if (dvsConfigInfo == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "is empty");
            return null;
        }

        if (!(dvsConfigInfo instanceof VMwareDVSConfigInfo)) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "isn't instanceof VMwareDVSConfigInfo");
            return null;
        }

        VMwareDVSPvlanMapEntry[] pvlanMapArray = dvsConfigInfo.getPvlanConfig();
        if (pvlanMapArray == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " Private VLAN NOT" +
                    "configured");
            return null;
        }

        // Populate VMware Virtual Network Info
        SortedMap<String, VmwareVirtualNetworkInfo> vnInfos =
                new TreeMap<String, VmwareVirtualNetworkInfo>();
        for (DistributedVirtualPortgroup dvPg : dvPgs) {
            String vnName = dvPg.getName();
            s_logger.debug("dvPg: " + vnName);
            // Extract dvPg configuration info and port setting
            DVPortgroupConfigInfo configInfo = dvPg.getConfig();
            DVPortSetting portSetting = configInfo.getDefaultPortConfig();

            // Ignore network?
            if (doIgnoreVirtualNetwork(portSetting)) {
                continue;
            }
            // Find associated IP Pool
            IpPool ipPool = getIpPool(dvPg, ipPools);
            if (ipPool == null) {
                s_logger.debug("no ip pool is associated to dvPg: " + vnName);
                continue;
            }
            byte[] vnKeyBytes = dvPg.getKey().getBytes();
            String vnUuid = UUID.nameUUIDFromBytes(vnKeyBytes).toString();
            s_logger.debug("VN name: " + vnName);
            IpPoolIpPoolConfigInfo ipConfigInfo = ipPool.getIpv4Config();

            // get pvlan/vlan info for the portgroup.
            HashMap<String, Short> vlan = getVlanInfo(dvPg, portSetting, pvlanMapArray);
            if (vlan == null) {
                s_logger.debug("no pvlan/vlan is associated to dvPg: " + vnName);
                return null;
            }
            short primaryVlanId   = vlan.get("primary-vlan");
            short isolatedVlanId  = vlan.get("secondary-vlan");

            // Read externalIpam flag from custom field
            boolean externalIpam = getExternalIpamInfo(configInfo, vnName);

            // Populate associated VMs
            SortedMap<String, VmwareVirtualMachineInfo> vmInfo = 
                    populateVirtualMachineInfo(dvPg, externalIpam);
            VmwareVirtualNetworkInfo vnInfo = new
                    VmwareVirtualNetworkInfo(vnName, isolatedVlanId, 
                            primaryVlanId, vmInfo,
                            ipConfigInfo.getSubnetAddress(),
                            ipConfigInfo.getNetmask(),
                            ipConfigInfo.getGateway(),
                            ipConfigInfo.getIpPoolEnabled(),
                            ipConfigInfo.getRange(),
                            externalIpam);
            vnInfos.put(vnUuid, vnInfo);
        }
        return vnInfos;
    }

    public IpPool getIpPool(DistributedVirtualPortgroup portGroup,
                            String dvPgName,
                            IpPool[] ipPools,
                            Integer poolid) {

        // If PG to IpPool association exists, check
        if (poolid != null) {
            for (IpPool pool : ipPools) {
                if (pool.id == poolid.intValue()) {
                  return pool;
                }
            }
        }

        // Validate that the IpPool name matches PG names 
        String IpPoolForPG = "ip-pool-for-" + dvPgName;
        for (IpPool pool : ipPools) {
            if (IpPoolForPG.equals(pool.getName())) {
                return pool;
            }
        }
        return null;
    }
 
    private String getVirtualMachineIpAddress(GuestNicInfo[] nicInfos, 
                                              String dvPgName, 
                                              String vmName, String vmMac)
                                             throws Exception {

        // Assumption here is that VMware Tools are installed
        // and IP address is available
        if (nicInfos == null) {
            s_logger.debug("dvPg: " + dvPgName + " vm:" + vmName
                    + " GuestNicInfo - VMware Tools " + " NOT installed");
            return null;
        }
        for (GuestNicInfo nicInfo : nicInfos) {
            // Extract the IP address associated with interface based on macAddress.
            String guestMac = nicInfo.getMacAddress();

            if (guestMac == null) {
                continue;
            }

            if (!guestMac.equals(vmMac)) {
                continue;
            }

            NetIpConfigInfo ipConfigInfo = nicInfo.getIpConfig();
            if (ipConfigInfo == null) {
                continue;
            }
            NetIpConfigInfoIpAddress[] ipAddrConfigInfos = 
                    ipConfigInfo.getIpAddress();
            if (ipAddrConfigInfos == null || 
                    ipAddrConfigInfos.length == 0) {
                continue;
            }

            for (NetIpConfigInfoIpAddress ipAddrConfigInfo : 
                ipAddrConfigInfos) {
                String ipAddress = ipAddrConfigInfo.getIpAddress();
                // Choose IPv4 only
                InetAddress ipAddr = InetAddress.getByName(ipAddress);
                if (ipAddr instanceof Inet4Address) {
                    return ipAddress;
                }
            }
        }
        return null;
    }

    VmwareVirtualMachineInfo fillVmwareVirtualMachineInfo(
                                       VirtualMachine vcenterVm,
                                       VmwareVirtualMachineInfo prevVmwareVmInfo,
                                       Hashtable pTable,
                                       DistributedVirtualPortgroup portGroup,
                                       String dvPgName,
                                       boolean externalIpam) throws Exception {
        // Name
        String vmName = (String) pTable.get("name");

        // Ignore virtual machine?
        if (doIgnoreVirtualMachine(vmName)) {
            s_logger.debug("dvPg: " + dvPgName +
                    " Ignoring vm: " + vmName);
            return null;
        }

        // Is it powered on?
        VirtualMachinePowerState powerState =(VirtualMachinePowerState)  pTable.get("runtime.powerState");

        // Extract MAC address & host/vrouter information
        String vmMac = null;
        ManagedObjectReference host_mor = null;
        String vrouterIpAddress = null;
        String hostName = null;
        ManagedObjectReference prev_hmor = null;

        if (prevVmwareVmInfo != null) {
            vmMac = prevVmwareVmInfo.getMacAddress();
            prev_hmor = prevVmwareVmInfo.getHmor();
        } else {
            vmMac = getVirtualMachineMacAddress(vcenterVm.getConfig(), portGroup);
            if (vmMac == null) {
                s_logger.error("dvPg: " + dvPgName + " vm: " + 
                        vmName + " MAC Address NOT found");
                return null;
            }
        }

        // Compare saved and current host reference
        ManagedObjectReference hmor = (ManagedObjectReference) pTable.get("runtime.host");
        if ((prev_hmor != null) && prev_hmor.getVal().equals(hmor.getVal())) {
            vrouterIpAddress = prevVmwareVmInfo.getVrouterIpAddress();
            hostName = prevVmwareVmInfo.getHostName();
        } else {
            HostSystem host = new HostSystem(
                vcenterVm.getServerConnection(), hmor);
            hostName = host.getName();

            // Get Contrail VRouter virtual machine information from the host
            vrouterIpAddress = getVRouterVMIpFabricAddress(
                    hostName, host, contrailVRouterVmNamePrefix);
            if (vrouterIpAddress == null) {
                s_logger.error("ContrailVM not found on ESXi host: " 
                        + hostName + ", skipping VM (" + vmName + ") creation"
                        + " on dvPg: " + dvPgName);
                return null;
            }
        }

        // found valid vm instance.
        VmwareVirtualMachineInfo vmInfo = new
                VmwareVirtualMachineInfo(vmName, hostName, hmor,
                        vrouterIpAddress, vmMac, powerState);

        // Save static-ip read via tools if static-ip addressing enabled on network.
        if ((externalIpam == true) && (vmInfo.isPoweredOnState())) {
            String ipAddress = null;
            String toolsRunningStatus  = (String)  pTable.get("guest.toolsRunningStatus");
            if (VirtualMachineToolsRunningStatus.guestToolsRunning.toString().equals(toolsRunningStatus)) {
                if (pTable.get("guest.net") instanceof GuestNicInfo[]) {
                    GuestNicInfo[] nicInfos    = (GuestNicInfo[])pTable.get("guest.net");
                    ipAddress = getVirtualMachineIpAddress(nicInfos, dvPgName, vmName, vmMac);
                }
            }

            if (ipAddress == null) {
                String prevIpAddress = null;

                if (prevVmwareVmInfo != null) {
                    prevIpAddress = prevVmwareVmInfo.getIpAddress();
                }

                if (VirtualMachineToolsRunningStatus.guestToolsRunning.toString().equals(toolsRunningStatus)
                    && prevIpAddress != null) {
                    // We have a problem here. Maybe the MOB is messed up
                    // VM had an IP before,but not now.
                    s_logger.error("Please restart vmware tools to ensure IP address is reported to vcenter");
                }
                if (prevIpAddress != null) {
                    ipAddress = prevIpAddress;
                    s_logger.debug("Using IP address:" + prevIpAddress + " read previously for VM ("
                                   + vmName + ") since vCenter didn't provide an current IP address");
                }
            }

            if (ipAddress != null) {
              // Ensure that ip-address is within subnet range
            }

            vmInfo.setIpAddress(ipAddress);
        }

        // everything done.
        return vmInfo;
    }

    private SortedMap<String, VmwareVirtualMachineInfo> 
        populateVirtualMachineInfoOptimized(
                DistributedVirtualPortgroup portGroup,
                String dvPgName,
                SortedMap<String, VmwareVirtualMachineInfo> prevVmwareVmInfos,
                boolean externalIpam) throws Exception {

        // Get list of virtual machines connected to the port group
        VirtualMachine[] vms = portGroup.getVms();

        if (vms == null || vms.length == 0) {
            s_logger.debug("dvPg: " + dvPgName + 
                    " NO virtual machines connected");
            return null;
        }

        // Read Virtual Machine Managed Object.
        Hashtable[] pTables = null;
        if (externalIpam == true) {
            pTables = PropertyCollectorUtil.retrieveProperties(vms, "VirtualMachine",
                    new String[] {"name",
                    "config.instanceUuid",
                    "runtime.powerState",
                    "runtime.host",
                    "guest.toolsRunningStatus",
                    "guest.net"
                    });
        } else {
            pTables = PropertyCollectorUtil.retrieveProperties(vms, "VirtualMachine",
                    new String[] {"name",
                    "config.instanceUuid",
                    "runtime.powerState",
                    "runtime.host",
                    });
        }

        SortedMap<String, VmwareVirtualMachineInfo> vmInfos =
                new TreeMap<String, VmwareVirtualMachineInfo>();
        for (int i=0; i < vms.length; i++) {

            // Check if previous vmware database has vm with this uuid
            String instanceUuid             = (String)  pTables[i].get("config.instanceUuid");
            VmwareVirtualMachineInfo prevVmwareVmInfo = null;
            if (prevVmwareVmInfos != null) {
                prevVmwareVmInfo = prevVmwareVmInfos.get(instanceUuid);
            }
            VmwareVirtualMachineInfo vmInfo = fillVmwareVirtualMachineInfo(vms[i],
                                                                           prevVmwareVmInfo,
                                                                           pTables[i],
                                                                           portGroup,
                                                                           dvPgName,
                                                                           externalIpam);
            if (vmInfo == null) {
                continue;
            }
            vmInfos.put(instanceUuid, vmInfo);
        }
        if (vmInfos.size() == 0) {
            return null;
        }
        return vmInfos;
    }

    public SortedMap<String, VmwareVirtualNetworkInfo>
        populateVirtualNetworkInfoOptimized() throws Exception {

        if (contrailDVS == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " NOT configured");
            return null;
        }
        // Extract distributed virtual port groups
        DistributedVirtualPortgroup[] dvPgs = contrailDVS.getPortgroup();
        if (dvPgs == null || dvPgs.length == 0) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Distributed portgroups NOT configured");
            return null;
        }

        // Get stored vcenter database from previous run
        SortedMap<String, VmwareVirtualNetworkInfo>
                    prevVmwareVNInfos = getPrevVmwareVNInfos();

        // Extract IP Pools
        IpPool[] ipPools = ipPoolManager.queryIpPools(contrailDC);
        if (ipPools == null || ipPools.length == 0) {
            s_logger.debug("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " IP Pools NOT " +
                    "configured");
            return null;
        }

        // Extract private vlan entries for the virtual switch
        VMwareDVSConfigInfo dvsConfigInfo = (VMwareDVSConfigInfo) contrailDVS.getConfig();
        if (dvsConfigInfo == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "is empty");
            return null;
        }

        if (!(dvsConfigInfo instanceof VMwareDVSConfigInfo)) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "isn't instanceof VMwareDVSConfigInfo");
            return null;
        }

        VMwareDVSPvlanMapEntry[] pvlanMapArray = dvsConfigInfo.getPvlanConfig();
        if (pvlanMapArray == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " Private VLAN NOT" +
                    "configured");
            return null;
        }

        Hashtable[] pTables = PropertyCollectorUtil.retrieveProperties(dvPgs,
                                "DistributedVirtualPortgroup",
                new String[] {"name",
                "config.key",
                "config.defaultPortConfig",
                "config.vendorSpecificConfig",
                "summary.ipPoolId",
                "summary.ipPoolName",
                });

        // Populate VMware Virtual Network Info
        SortedMap<String, VmwareVirtualNetworkInfo> vnInfos =
                new TreeMap<String, VmwareVirtualNetworkInfo>();
        for (int i=0; i < dvPgs.length; i++) {
            short primaryVlanId;
            short isolatedVlanId;
            String subnetAddress;
            String subnetMask;
            String gatewayAddress;
            boolean ipPoolEnabled;
            String range;
            boolean externalIpam;
            VmwareVirtualNetworkInfo prevVmwareVNInfo =null;
            SortedMap<String, VmwareVirtualMachineInfo> prevVmwareVmInfos = null;

            String vnName = (String) pTables[i].get("name");
            s_logger.debug("dvPg: " + vnName);

            String key = (String) pTables[i].get("config.key");
            byte[] vnKeyBytes = key.getBytes();
            String vnUuid = UUID.nameUUIDFromBytes(vnKeyBytes).toString();

            // Extract dvPg configuration info and port setting
            DVPortSetting portSetting = (DVPortSetting) pTables[i].get("config.defaultPortConfig");

            // Ignore network?
            if (doIgnoreVirtualNetwork(portSetting)) {
                continue;
            }

            // Check if the network was created in the previous run of periodic
            if (prevVmwareVNInfos != null) {
                prevVmwareVNInfo = prevVmwareVNInfos.get(vnUuid);
            }

            if (prevVmwareVNInfo != null) {
                prevVmwareVmInfos = prevVmwareVNInfo.getVmInfo();
            }

            // Find associated IP Pool
            Integer poolId     = (Integer) pTables[i].get("summary.ipPoolId");
            IpPool ipPool = getIpPool(dvPgs[i], vnName, ipPools, poolId);
            if (ipPool == null) {
                s_logger.debug("no ip pool is associated to dvPg: " + vnName);
                continue;
            }

            IpPoolIpPoolConfigInfo ipConfigInfo = ipPool.getIpv4Config();

            // get pvlan/vlan info for the portgroup.
            HashMap<String, Short> vlan = getVlanInfo(dvPgs[i], portSetting, pvlanMapArray);
            if (vlan == null) {
                s_logger.debug("no pvlan/vlan is associated to dvPg: " + vnName);
                return null;
            }
            s_logger.debug("VN name: " + vnName);

            primaryVlanId   = vlan.get("primary-vlan");
            isolatedVlanId  = vlan.get("secondary-vlan");

            // ifconfig setting
            subnetAddress  = ipConfigInfo.getSubnetAddress();
            subnetMask     = ipConfigInfo.getNetmask();
            gatewayAddress = ipConfigInfo.getGateway();
            ipPoolEnabled  = ipConfigInfo.getIpPoolEnabled();
            range          = ipConfigInfo.getRange();

            // Read externalIpam flag from custom field
            DistributedVirtualSwitchKeyedOpaqueBlob[] opaqueBlobs = null;
            Object obj = pTables[i].get("config.vendorSpecificConfig");
            if (obj instanceof DistributedVirtualSwitchKeyedOpaqueBlob[]) {
                opaqueBlobs = (DistributedVirtualSwitchKeyedOpaqueBlob[]) obj;
            }
            externalIpam = getExternalIpamInfo(opaqueBlobs, vnName);

            // Populate associated VMs
            SortedMap<String, VmwareVirtualMachineInfo> vmInfo =
                    populateVirtualMachineInfoOptimized(dvPgs[i], vnName, prevVmwareVmInfos, externalIpam);

            VmwareVirtualNetworkInfo vnInfo = new
                    VmwareVirtualNetworkInfo(vnName, isolatedVlanId, 
                            primaryVlanId, vmInfo,
                            subnetAddress,
                            subnetMask,
                            gatewayAddress,
                            ipPoolEnabled,
                            range,
                            externalIpam);
            vnInfos.put(vnUuid, vnInfo);
        }
        return vnInfos;
    }
    
    public String getVcenterUrl() { 
        return vcenterUrl; 
    }
    
    public Datacenter getVmwareDatacenter(String name)
        throws RemoteException {
        String description = "<datacenter " + name
                + ", vCenter " + vcenterUrl + ">.";

        if (datacenters.containsKey(name)) {
            s_logger.info("Found in cache " + description);
            return datacenters.get(name);
        }

        Folder rootFolder = serviceInstance.getRootFolder();
        if (rootFolder == null) {
            String msg = "Failed to get rootfolder for vCenter " + vcenterUrl;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }
        InventoryNavigator inventoryNavigator = new InventoryNavigator(rootFolder);
        Datacenter dc = null;
        try {
            dc = (Datacenter) inventoryNavigator.searchManagedEntity(
                          "Datacenter", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (dc == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        datacenters.put(name, dc);

        s_logger.info("Found " + description);
        return dc;
    }

    public VmwareDistributedVirtualSwitch getVmwareDvs(String name,
            Datacenter dc, String dcName)
                    throws RemoteException {
        String description = "<dvs " + name
                + ", datacenter " + dcName
                + ", vCenter " + vcenterUrl + ">.";

        if (dvswitches.containsKey(name)) {
            s_logger.info("Found in cache " + description);
            return dvswitches.get(name);
        }

        InventoryNavigator inventoryNavigator = new InventoryNavigator(dc);

        VmwareDistributedVirtualSwitch dvs = null;
        try {
            dvs = (VmwareDistributedVirtualSwitch)inventoryNavigator.searchManagedEntity(
                    "VmwareDistributedVirtualSwitch", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (dvs == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        s_logger.info("Found " + description);
        dvswitches.put(name, dvs);
        return dvs;
    }

    public Network getVmwareNetwork(String name,
            VmwareDistributedVirtualSwitch dvs, String dvsName, String dcName)
            throws RemoteException {
        String description = "<network " + name
                + ", dvs " + dvsName + ", datacenter " + dcName
                + ", vCenter " + vcenterUrl + ">.";

        // funny but search on the dvs does not work, we need to use rootFolder
        InventoryNavigator inventoryNavigator = new InventoryNavigator(rootFolder);

        Network nw = null;
        try {
            nw = (Network)inventoryNavigator.searchManagedEntity(
                    "Network", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (nw == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        s_logger.info("Found " + description);
        return nw;
    }

    public DistributedVirtualPortgroup getVmwareDpg(String name,
            VmwareDistributedVirtualSwitch dvs, String dvsName, String dcName)
            throws RemoteException {
        String description = "<dpg " + name + ", dvs " + dvsName
                + ", datacenter " + dcName + ", vCenter " + vcenterUrl + ">.";

        // funny but search on the dvs does not work, we need to use rootFolder
        InventoryNavigator inventoryNavigator = new InventoryNavigator(rootFolder);

        DistributedVirtualPortgroup dpg = null;
        try {
            dpg = (DistributedVirtualPortgroup)inventoryNavigator.searchManagedEntity(
                    "DistributedVirtualPortgroup", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (dpg == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        s_logger.info("Found " + description);
        return dpg;
    }

    public HostSystem getVmwareHost(String name,
            Datacenter dc, String dcName)
        throws RemoteException {
        String description = "<host " + name 
                + ", datacenter " + dcName + ", vCenter " + vcenterUrl +">.";
        // narrow the search to the dc level
        InventoryNavigator inventoryNavigator = new InventoryNavigator(dc);

        HostSystem host = null;
        try {
            host = (HostSystem)inventoryNavigator.searchManagedEntity(
                    "HostSystem", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (host == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        s_logger.info("Found " + description);
        return host;
    }

    public VirtualMachine getVmwareVirtualMachine(String name,
            HostSystem host, String hostName, String dcName) 
        throws RemoteException {
        String description = "<virtual machine " + name + ", host " + hostName 
                + ", datacenter " + dcName + ", vCenter " + vcenterUrl +">.";
        // narrow the search to the host level
        InventoryNavigator inventoryNavigator = new InventoryNavigator(host);

        VirtualMachine vm = null;
        try {
            vm = (VirtualMachine)inventoryNavigator.searchManagedEntity(
                    "VirtualMachine", name);
        } catch (RemoteException e ) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg, e);
        }

        if (vm == null) {
            String msg = "Failed to retrieve " + description;
            s_logger.error(msg);
            throw new RemoteException(msg);
        }

        s_logger.info("Found " + description);
        return vm;
    }
      
    public void readAllVirtualNetworks(SortedMap<String, VmwareVirtualNetworkInfo> map) 
            throws Exception {

        if (contrailDVS == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " NOT configured");
            return;
        }
        // Extract distributed virtual port groups
        DistributedVirtualPortgroup[] dvPgs = contrailDVS.getPortgroup();
        if (dvPgs == null || dvPgs.length == 0) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Distributed portgroups NOT configured");
            return;
        }
        
        // Extract IP Pools
        IpPool[] ipPools = ipPoolManager.queryIpPools(contrailDC);
        if (ipPools == null || ipPools.length == 0) {
            s_logger.debug("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " IP Pools NOT " +
                    "configured");
            return;
        }
    
        // Extract private vlan entries for the virtual switch
        VMwareDVSConfigInfo dvsConfigInfo = (VMwareDVSConfigInfo) contrailDVS.getConfig();
        if (dvsConfigInfo == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "is empty");
            return;
        }
    
        if (!(dvsConfigInfo instanceof VMwareDVSConfigInfo)) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " ConfigInfo " +
                    "isn't instanceof VMwareDVSConfigInfo");
            return;
        }
    
        VMwareDVSPvlanMapEntry[] pvlanMapArray = dvsConfigInfo.getPvlanConfig();
        if (pvlanMapArray == null) {
            s_logger.error("dvSwitch: " + contrailDvSwitchName +
                    " Datacenter: " + contrailDC.getName() + " Private VLAN NOT" +
                    "configured");
            return;
        }
    
        Hashtable[] pTables = PropertyCollectorUtil.retrieveProperties(dvPgs,
                                "DistributedVirtualPortgroup",
                new String[] {"name",
                "config.key",
                "config.defaultPortConfig",
                "config.vendorSpecificConfig",
                "summary.ipPoolId",
                "summary.ipPoolName",
                });
    
        for (int i=0; i < dvPgs.length; i++) {
         // Extract dvPg configuration info and port setting
            DVPortSetting portSetting = (DVPortSetting) pTables[i].get("config.defaultPortConfig");

            if (doIgnoreVirtualNetwork(portSetting)) {
                continue;
            }
            VmwareVirtualNetworkInfo vnInfo = 
                    new VmwareVirtualNetworkInfo(
                            this, dvPgs[i], pTables[i], contrailDC, contrailDataCenterName, 
                            contrailDVS, contrailDvSwitchName,
                            ipPools, pvlanMapArray);
            
            map.put(vnInfo.getUuid(), vnInfo);
        }
    }

    private void readAllVirtualMachines(SortedMap<String, VmwareVirtualMachineInfo> map,
            ManagedEntity me, 
            Datacenter dc, String dcName) 
                throws Exception {

        ManagedEntity[] vms = new InventoryNavigator(me).searchManagedEntities("VirtualMachine"); 
        
        if (vms == null || vms.length == 0) {
            s_logger.debug("Datacenter: " + dcName + 
                    " NO virtual machines connected");
            return;
        }
    
        Hashtable[] pTables = PropertyCollectorUtil.retrieveProperties(vms, "VirtualMachine",
                new String[] {"name",
                "config.instanceUuid",
                "runtime.powerState",
                "runtime.host",
                "guest.toolsRunningStatus",
                "guest.net"
                });
        
        for (int i=0; i < vms.length; i++) {   
            VmwareVirtualMachineInfo vmInfo = new VmwareVirtualMachineInfo(this,
                    dc, dcName,
                    (VirtualMachine)vms[i], pTables[i]);
            
            // Ignore virtual machine?
            if (doIgnoreVirtualMachine(vmInfo.getName())) {
                s_logger.debug(" Ignoring vm: " + vmInfo.getName());
                continue;
            }
            
            if (vmInfo.getVmiInfo().size() == 0) {
                // this can happen if the VM is not connected to any network
                // By design we skip unconnected VMs
                continue;
            }
            map.put(vmInfo.getUuid(), vmInfo);
        }
    }
    
    void readAllVirtualMachines(SortedMap<String, VmwareVirtualMachineInfo> map) 
            throws Exception {
        
        /* the method below can be called in a loop to read multiple  
         * datacenters and read VMs per hosts     
         * for (dc: datacenters)
         * for (host: dc)
            readAllVirtualMachines(host, dc, dcName);
         */
        readAllVirtualMachines(map, contrailDC, contrailDC, contrailDataCenterName);
    }
    
    public void init() throws Exception{
        while (Initialize() != true) {
            Thread.sleep(2);
        }
        while (Initialize_data() != true) {
            Thread.sleep(2);
        }
    }
}
