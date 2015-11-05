package net.juniper.contrail.vcenter;

import com.vmware.vim25.VirtualMachinePowerState;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.vmware.vim25.ManagedObjectReference;
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
    private SortedMap<String, VmwareVirtualMachineInterfaceInfo> vmiInfo;
    
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

        vmiInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
    }

    public VmwareVirtualMachineInfo(String uuid) {
        this.uuid = uuid;
        vmiInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
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
        return vmiInfo;
    }

    public void setVmiInfo(SortedMap<String, VmwareVirtualMachineInterfaceInfo> vmiInfo) {
        this.vmiInfo = vmiInfo;
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
        if (vmiInfo.size() != vm.vmiInfo.size()) {
            return false;
        }
        
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter1 =
                vmiInfo.entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter2 =
                vm.vmiInfo.entrySet().iterator();
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
}
