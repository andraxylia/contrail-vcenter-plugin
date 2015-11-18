/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import net.juniper.contrail.api.types.VirtualNetwork;
import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualMachineInterface;

import java.io.IOException;

import net.juniper.contrail.api.types.InstanceIp;

public class VmwareVirtualMachineInterfaceInfo extends VCenterObject {
    private String uuid;
    VmwareVirtualMachineInfo vmInfo;
    VmwareVirtualNetworkInfo vnInfo;
    private String ipAddress;
    private String macAddress;
    
    //API server objects
    net.juniper.contrail.api.types.VirtualMachineInterface apiVmi;
    net.juniper.contrail.api.types.InstanceIp apiInstanceIp;

    VmwareVirtualMachineInterfaceInfo(VmwareVirtualMachineInfo vmInfo,
            VmwareVirtualNetworkInfo vnInfo) {
        this.vmInfo = vmInfo;
        this.vnInfo = vnInfo;
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
        
        if (!vmInfo.getUuid().equals(vmi.vmInfo.getUuid())) {
            return false;
        }
        
        if (!vnInfo.getUuid().equals(vmi.vnInfo.getUuid())) {
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
        return true;
    }

    public String toString() {
        return "VMI <VM " + vmInfo.getName() + ", VN " + vnInfo.getName() 
            + ", " + uuid + ">";
    }
    
    @Override
    void create(VncDB vncDB) throws Exception {
        
        vncDB.updateVirtualMachineInterface(this);
        
        // comment temporarily for testing
        /*
        if (vmInfo.isPoweredOnState()) {
            VRouterNotifier.addPort(this);
        }*/
    }

    @Override
    void update(VCenterObject obj,
            VncDB vncDB) throws Exception {
        
        VmwareVirtualMachineInterfaceInfo newVmiInfo = (VmwareVirtualMachineInterfaceInfo)obj;
        
        if (equals(newVmiInfo)) {
            // nothing has changed
            return;
        }
        
        boolean updateVRouterNeeded = false;
        
        if (newVmiInfo.ipAddress != null && !newVmiInfo.ipAddress.equals(ipAddress)) {
            ipAddress = newVmiInfo.ipAddress;
            updateVRouterNeeded = true;
        }
        if (newVmiInfo.macAddress != null && !newVmiInfo.macAddress.equals(macAddress)) {
            macAddress = newVmiInfo.macAddress;
            updateVRouterNeeded = true;
        }
        
        // comment temporarily for testing
        /*
        if (updateVRouterNeeded && vmInfo.isPoweredOnState()) {
            VRouterNotifier.deletePort(this);
            VRouterNotifier.addPort(this);
        }*/
    }

    @Override
    void delete(VncDB vncDB) 
            throws IOException {
        /*
        if (vmInfo.isPoweredOnState()) {
            VRouterNotifier.deletePort(this);
        }
        */
        
        vncDB.deleteVirtualMachineInterface(this);
    }
}
