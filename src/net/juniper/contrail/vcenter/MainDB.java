/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MainDB {
    private static volatile SortedMap<String, VmwareVirtualNetworkInfo> vmwareVNs =
            new ConcurrentSkipListMap<String, VmwareVirtualNetworkInfo>();
    private static volatile SortedMap<String, VmwareVirtualMachineInfo> vmwareVMs =
            new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
    

    public static void updateVM(VmwareVirtualMachineInfo vmInfo) {
        vmwareVMs.put(vmInfo.getUuid(), vmInfo);
    }

    public static void deleteVM(VmwareVirtualMachineInfo vmInfo) {
        String uuid = vmInfo.getUuid();

        if (!vmwareVMs.containsKey(uuid)) {
            return;
        }
        vmwareVMs.remove(uuid);
    }

    public static void updateVN(VmwareVirtualNetworkInfo vnInfo) {
        vmwareVNs.put(vnInfo.getUuid(), vnInfo);
    }

    public static void deleteVN(VmwareVirtualNetworkInfo vnInfo) {
        String uuid = vnInfo.getUuid();

        if (!vmwareVNs.containsKey(uuid)) {
            return;
        }
        vmwareVNs.remove(uuid);
    }

    public static VmwareVirtualNetworkInfo getVnByName(String name) {
        for (VmwareVirtualNetworkInfo vnInfo: vmwareVNs.values()) {
            if (vnInfo.getName().equals(name)) {
                return vnInfo;
            }
        }
        return null;
    }

    public static VmwareVirtualNetworkInfo getVnById(String uuid) {
        if (vmwareVNs.containsKey(uuid)) {
            return vmwareVNs.get(uuid);
        }
        return null;
    }
    
    public static VmwareVirtualMachineInfo getVmById(String uuid) {
        if (vmwareVMs.containsKey(uuid)) {
            return vmwareVMs.get(uuid);
        }
        return null;
    }
    
    public static void init(VCenterDB vcenterDB, VncDB vncDB, String mode) 
            throws Exception {
        vmwareVNs.clear();
        vmwareVMs.clear();
        
        vcenterDB.readAllVirtualNetworks(vmwareVNs);
        vcenterDB.readAllVirtualMachines(vmwareVMs);
        
        if (mode == "vcenter-only") {
            vncDB.syncVirtualNetworks(vmwareVNs);
            vncDB.syncVirtualMachines(vmwareVMs);
        }
        
        printInfo();
    }
    
    public static void printInfo() {
        System.out.println("Networks:");
        for (VmwareVirtualNetworkInfo vnInfo: vmwareVNs.values()) {
            System.out.println(vnInfo.toStringBuffer().toString());
        }
        
        System.out.println("VMs:");
        for (VmwareVirtualMachineInfo vmInfo: vmwareVMs.values()) {
            System.out.println(vmInfo.toStringBuffer().toString());
        }
    }
}
