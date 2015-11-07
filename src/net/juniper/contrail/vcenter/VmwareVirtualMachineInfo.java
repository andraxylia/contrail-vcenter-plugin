package net.juniper.contrail.vcenter;

import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.VirtualMachineToolsRunningStatus;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.Network;

import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import com.vmware.vim25.Event;
import com.vmware.vim25.GuestNicInfo;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.RuntimeFault;
import net.juniper.contrail.api.types.VirtualMachine;

public class VmwareVirtualMachineInfo {
    private String uuid; // required attribute, key for this object
    ManagedObjectReference hmor;
    private String hostName;
    private String vrouterIpAddress;
    private String macAddress;
    private String ipAddress;
    private String name;
    private String interfaceUuid;
    private VirtualMachinePowerState powerState;
    private SortedMap<String, VmwareVirtualMachineInterfaceInfo> vmiInfoMap;
    
    // Vmware objects
    com.vmware.vim25.mo.VirtualMachine vm;
    com.vmware.vim25.mo.HostSystem host;
    com.vmware.vim25.mo.VmwareDistributedVirtualSwitch dvs;
    String dvsName;
    com.vmware.vim25.mo.Datacenter dc;
    String dcName; 
    
    //API server objects
    net.juniper.contrail.api.types.VirtualMachine apiVm;

    public VmwareVirtualMachineInfo(String name, String hostName, 
            ManagedObjectReference hmor,
            String vrouterIpAddress, String macAddress,
            VirtualMachinePowerState powerState) {
        this.name             = name;
        this.hostName         = hostName;
        this.vrouterIpAddress = vrouterIpAddress;
        this.macAddress       = macAddress;
        this.powerState       = powerState;
        this.hmor             = hmor;

        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
    }

    public VmwareVirtualMachineInfo(String uuid) {
        this.uuid = uuid;
        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
    }

    public VmwareVirtualMachineInfo(Event event,  VCenterDB vcenterDB) throws Exception {
        if (event.getDatacenter() != null) {
            dcName = event.getDatacenter().getName();
            dc = vcenterDB.getVmwareDatacenter(dcName);
        }

        if (event.getDvs() != null) {
            dvsName = event.getDvs().getName();
            dvs = vcenterDB.getVmwareDvs(dvsName, dc, dcName);
        } else {
            dvsName = vcenterDB.contrailDvSwitchName;
            dvs = vcenterDB.getVmwareDvs(dvsName, dc, dcName);
        }

        if (event.getHost() != null) {
            hostName = event.getHost().getName();
            host = vcenterDB.getVmwareHost(hostName, dc, dcName);

            if (event.getVm() != null) {
                name = event.getVm().getName();
  
                vm = vcenterDB.getVmwareVirtualMachine(name, host, hostName, dcName);
             }
        }
        
        vrouterIpAddress = vcenterDB.getVRouterVMIpFabricAddress(
                VCenterDB.contrailVRouterVmNamePrefix,
                host, hostName);
        
        
        uuid = vm.getConfig().getInstanceUuid();
        

        VirtualMachineRuntimeInfo vmRuntimeInfo = vm.getRuntime();
        powerState = vmRuntimeInfo.getPowerState();

        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
        populateVmiInfoMap(vcenterDB);

    }

    public VmwareVirtualMachineInfo(VCenterDB vcenterDB,
            com.vmware.vim25.mo.Datacenter dc, String dcName,
            com.vmware.vim25.mo.VirtualMachine vm, Hashtable pTable) 
                    throws Exception {
        

        if (vcenterDB == null || dc == null || dcName == null
                || vm == null || pTable == null) {
            throw new IllegalArgumentException();
        }
        
        this.dc = dc;
        this.dcName = dcName;
        this.vm = vm;
        
        // Name
        name = (String) pTable.get("name");

        ManagedObjectReference hostHmor = (ManagedObjectReference) pTable.get("runtime.host");
        host = new HostSystem(
            vm.getServerConnection(), hostHmor);
        hostName = host.getName();

        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
        populateVmiInfoMap(vcenterDB);
    }
    
    private void populateVmiInfoMap(VCenterDB vcenterDB) throws Exception {
        Network[] nets = vm.getNetworks();
        
        for (Network net: nets) {
            String netName = net.getName();
            VmwareVirtualNetworkInfo vnInfo = vcenterDB.getVnByName(netName);
            if (vnInfo == null) {
                continue;
            }
   
            String vmiUuid = UUID.randomUUID().toString();
            VmwareVirtualMachineInterfaceInfo vmiInfo = 
                    new VmwareVirtualMachineInterfaceInfo(vmiUuid);
            vmiInfo.setVmInfo(this);
            vmiInfo.setVnInfo(vnInfo);
            
            // Extract MAC address
            macAddress = VCenterDB.getVirtualMachineMacAddress(vm.getConfig(), vnInfo.getDpg());
            vmiInfo.setMacAddress(macAddress);
            ipAddress = vcenterDB.getVirtualMachineIpAddress(vm, vnInfo.getName());
            vmiInfo.setIpAddress(ipAddress);
            
            vmiInfoMap.put(vmiInfo.getUuid(), vmiInfo);
        }
    }
    
    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public ManagedObjectReference getHmor() {
        return hmor;
    }

    public void setHmor(ManagedObjectReference hmor) {
        this.hmor = hmor;
    }

    public String getVrouterIpAddress() {
        return vrouterIpAddress;
    }

    public void setVrouterIpAddress(String vrouterIpAddress) {
        this.vrouterIpAddress = vrouterIpAddress;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInterfaceUuid() {
        return interfaceUuid;
    }

    public void setInterfaceUuid(String uuid) {
        this.interfaceUuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public VirtualMachinePowerState getPowerState() {
        return powerState;
    }

    public void setPowerState(VirtualMachinePowerState powerState) {
        this.powerState = powerState;
    }

    public boolean isPowerStateEqual(VirtualMachinePowerState powerState) {
        if (this.powerState == powerState)
            return true;
        else
            return false;
    }
    public boolean isPoweredOnState() {
        if (powerState == VirtualMachinePowerState.poweredOn)
            return true;
        else
            return false;
    }

    public SortedMap<String, VmwareVirtualMachineInterfaceInfo> getVmiInfo() {
        return vmiInfoMap;
    }

    public void setVmiInfo(SortedMap<String, VmwareVirtualMachineInterfaceInfo> vmiInfoMap) {
        this.vmiInfoMap = vmiInfoMap;
    }

    public boolean updateVrouterNeeded(VmwareVirtualMachineInfo vm) {
        if (vm == null) {
            return true;
        }
        
        // for now all fields trigger an update
        return (!equals(vm));
    }

    public boolean equals(VmwareVirtualMachineInfo vm) {
        if (vm == null) {
            return false;
        }
        if ((uuid != null && !uuid.equals(vm.uuid))
                || (uuid == null && vm.uuid != null)) {
            return false;
        }
        if ((name != null && !name.equals(vm.name))
                || (name == null && vm.name != null)) {
            return false;
        }
        if ((hmor != null && !hmor.equals(vm.hmor))
                || (hmor == null && vm.hmor != null)) {
            return false;
        }
        if ((vrouterIpAddress != null && !vrouterIpAddress.equals(vm.vrouterIpAddress))
                || (vrouterIpAddress == null && vm.vrouterIpAddress != null)) {
            return false;
        }
        if ((hostName != null && !hostName.equals(vm.hostName))
                || (hostName == null && vm.hostName != null)) {
            return false;
        }
        if ((ipAddress != null && !ipAddress.equals(vm.ipAddress))
                || (ipAddress == null && vm.ipAddress != null)) {
            return false;
        }
        if ((macAddress != null && !macAddress.equals(vm.macAddress))
                || (macAddress == null && vm.macAddress != null)) {
            return false;
        }
        if ((powerState != null && !powerState.equals(vm.powerState))
                || (powerState == null && vm.powerState != null)) {
            return false;
        }
        return equalVmi(vm);
    }

    public boolean equalVmi(VmwareVirtualMachineInfo vm) {
        if (vm == null) {
            return false;
        }
        
        if (vmiInfoMap.size() != vm.vmiInfoMap.size()) {
            return false;
        }
        
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter1 =
                vmiInfoMap.entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter2 =
                vm.vmiInfoMap.entrySet().iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry1 = iter1.next();
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry2 = iter2.next();

            if (!entry1.getKey().equals(entry2.getKey())
                    || !entry1.getValue().equals(entry2.getValue())) {
                return false;
            }
        }
        return true;
    }

    public String toString() {
        return "VM <" + name + ", host " + hostName + ", " + uuid + ">";
    }
    
    public StringBuffer toStringBuffer() {
        StringBuffer s = new StringBuffer(
                "VM <" + name + ", host " + hostName + ", " + uuid + ">\n");
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter =
                vmiInfoMap.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter.next();
            s.append(entry.getValue().toString());
        }
        return s;
    }
    
    boolean ignore() {
        if (vrouterIpAddress == null) {
            return true;
        }
        
        return false;
    }
}
