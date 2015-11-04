/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import net.juniper.contrail.api.types.VirtualNetwork;
import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualMachineInterface;
import net.juniper.contrail.api.types.InstanceIp;

public class VmwareVirtualMachineInterfaceInfo {
    private String uuid;
    VmwareVirtualMachineInfo vmInfo;
    VmwareVirtualNetworkInfo vnInfo;
    private String ipAddress;
    private String macAddress;
    
    //API server objects
    net.juniper.contrail.api.types.VirtualMachineInterface apiVmi;
    net.juniper.contrail.api.types.InstanceIp apiInstanceIp;

    VmwareVirtualMachineInterfaceInfo(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public VmwareVirtualMachineInfo getVmInfo() {
        return vmInfo;
    }

    public void setVmInfo(VmwareVirtualMachineInfo vmInfo) {
        this.vmInfo = vmInfo;
    }

    public VmwareVirtualNetworkInfo getVnInfo() {
        return vnInfo;
    }

    public void setVnInfo(VmwareVirtualNetworkInfo vnInfo) {
        this.vnInfo = vnInfo;
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

    public boolean equals(VmwareVirtualMachineInterfaceInfo vmi) {
        if (vmi == null) {
            return false;
        }
        if ((ipAddress != null && !ipAddress.equals(vmi.ipAddress))
                || (ipAddress == null && vmi.ipAddress != null)) {
            return false;
        }
        if ((macAddress != null && !macAddress.equals(vmi.macAddress))
                || (macAddress == null && vmi.macAddress != null)) {
            return false;
        }
        return vnInfo.equals(vmi.vnInfo);
    }

    public String toString() {
        return "VMI <VM " + vmInfo.getName() + ", VN " + vnInfo.getName() + ">";
    }
}
