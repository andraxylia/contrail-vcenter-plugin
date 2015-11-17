/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualMachineInterface;
import net.juniper.contrail.api.types.VirtualNetwork;

public class MainDB {
    private static volatile SortedMap<String, VmwareVirtualNetworkInfo> vmwareVNs =
            new ConcurrentSkipListMap<String, VmwareVirtualNetworkInfo>();
    private static volatile SortedMap<String, VmwareVirtualMachineInfo> vmwareVMs =
            new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
    
    private static volatile VncDB vncDB;
    private static volatile VCenterDB vcenterDB;

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
    
   private static void syncVirtualNetworks(
           SortedMap<String, VmwareVirtualNetworkInfo> oldMap,
           SortedMap<String, VmwareVirtualNetworkInfo> newMap) {
        
        Iterator<Entry<String, VmwareVirtualNetworkInfo>> newIter = newMap.entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualNetworkInfo>> oldIter = oldMap.entrySet().iterator();
        Entry<String, VmwareVirtualNetworkInfo> oldEntry = oldIter.hasNext()? oldIter.next() : null;
        Entry<String, VmwareVirtualNetworkInfo> newEntry = newIter.hasNext()? newIter.next() : null;
        
        while (oldEntry != null && newEntry != null) {
            Integer cmp = newEntry.getKey().compareTo(oldEntry.getKey());
            try {
                if (cmp == 0) {
                    updateVirtualNetwork(oldEntry.getValue(), newEntry.getValue());
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else if (cmp < 0) {
                    updateVirtualNetwork(null, newEntry.getValue());
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else {
                    deleteVirtualNetwork(oldEntry.getValue());
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                }
            } catch (Exception e) {
                //s_logger.error("Cannot sync VN " + entry1.getKey());
            }
        }

        while (oldEntry != null) {
            try {
                deleteVirtualNetwork(oldEntry.getValue());
            } catch (Exception e) {
                //s_logger.error("Cannot delete VN " + entry2.getKey());
            }
            oldEntry = oldIter.hasNext()? oldIter.next() : null;
        }
        
        while (newEntry != null) {
            try {
                updateVirtualNetwork(newEntry.getValue(), null);
            } catch (Exception e) {
                //s_logger.error("Cannot create VN " + entry1.getKey());
            }
            newEntry = newIter.hasNext()? newIter.next() : null;
        }
    }

   /* parametrize after testing 
   private static <K extends Comparable,V> void syncVirtualNetworks(
           SortedMap<K, V> oldMap,
           SortedMap<K, V> newMap) {
        
        Iterator<Entry<K, V>> newIter = newMap.entrySet().iterator();
        Iterator<Entry<K, V>> oldIter = oldMap.entrySet().iterator();
        Entry<K, V> oldEntry = oldIter.hasNext()? oldIter.next() : null;
        Entry<K, V> newEntry = newIter.hasNext()? newIter.next() : null;
        
        while (oldEntry != null && newEntry != null) {
            Integer cmp = newEntry.getKey().compareTo(oldEntry.getKey());
            try {
                if (cmp == 0) {
                    update(oldEntry.getValue(), newEntry.getValue());
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else if (cmp < 0) {
                    update(null, newEntry.getValue());
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else {
                    delete(oldEntry.getValue());
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                }
            } catch (Exception e) {
                //s_logger.error("Cannot sync " + entry1.getKey());
            }
        }

        while (oldEntry != null) {
            try {
                delete(oldEntry.getValue());
            } catch (Exception e) {
                //s_logger.error("Cannot delete " + entry2.getKey());
            }
            oldEntry = oldIter.hasNext()? oldIter.next() : null;
        }
        
        while (newEntry != null) {
            try {
                update(newEntry.getValue(), null);
            } catch (Exception e) {
                //s_logger.error("Cannot create VN " + entry1.getKey());
            }
            newEntry = newIter.hasNext()? newIter.next() : null;
        }
    }
    
    */

    private static void syncVirtualMachines(
            SortedMap<String, VmwareVirtualMachineInfo> oldMap,
            SortedMap<String, VmwareVirtualMachineInfo> newMap) {
        
        Iterator<Entry<String, VmwareVirtualMachineInfo>> newIter = newMap.entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualMachineInfo>> oldIter = oldMap.entrySet().iterator();
        
        Entry<String, VmwareVirtualMachineInfo> oldEntry = oldIter.hasNext() ? oldIter.next() : null;
        Entry<String, VmwareVirtualMachineInfo> newEntry = newIter.hasNext() ? newIter.next() : null;
        
        while (oldEntry != null && newEntry != null) {
            
            Integer cmp = newEntry.getKey().compareTo(oldEntry.getKey());
            try {
                if (cmp == 0) {
                    updateVirtualMachine(oldEntry.getValue(), newEntry.getValue());
                    oldEntry = oldIter.hasNext() ? oldIter.next() : null;
                    newEntry = newIter.hasNext() ? newIter.next() : null;
                } else if (cmp < 0) {
                    updateVirtualMachine(newEntry.getValue());
                    newEntry = newIter.hasNext() ? newIter.next() : null;
                } else {
                    deleteVirtualMachine(oldEntry.getValue());
                    oldEntry = oldIter.hasNext() ? oldIter.next() : null;
                }
            } catch (Exception e) {
                //s_logger.error("Cannot sync VM ");
            }
        }

        while (oldEntry != null) {
            try {
                deleteVirtualMachine(oldEntry.getValue());
            } catch (Exception e) {
                //s_logger.error("Cannot delete VM ");
            }
            oldEntry = oldIter.hasNext() ? oldIter.next() : null;
        }
        
        while (newEntry != null) {
            try {
                updateVirtualMachine(newEntry.getValue());
            } catch (Exception e) {
                //s_logger.error("Cannot create VN " + entry1.getKey());
            }
            newEntry = newIter.hasNext() ? newIter.next() : null;
        }
    }

    private static void syncVirtualMachineInterfaces(VmwareVirtualMachineInfo oldVmInfo,
            VmwareVirtualMachineInfo newVmInfo) throws IOException {
        boolean updateVrouterNeeded = newVmInfo.updateVrouterNeeded(oldVmInfo);

        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> oldIter =
                oldVmInfo.getVmiInfo().entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> newIter =
                newVmInfo.getVmiInfo().entrySet().iterator();
        
        Entry<String, VmwareVirtualMachineInterfaceInfo> oldEntry = oldIter.hasNext() ? oldIter.next() : null;
        Entry<String, VmwareVirtualMachineInterfaceInfo> newEntry = newIter.hasNext() ? newIter.next() : null;
        
        while (oldEntry != null && newEntry != null) {
            VmwareVirtualMachineInterfaceInfo oldVmiInfo = oldEntry.getValue();
            VmwareVirtualMachineInterfaceInfo newVmiInfo = newEntry.getValue();
            Integer cmp = newEntry.getKey().compareTo(oldEntry.getKey());
            
            if (cmp == 0) {
                // same network
                if (updateVrouterNeeded) {
                    deleteVirtualMachineInterface(oldVmiInfo);
                    updateVirtualMachineInterface(newVmiInfo); 
                }
                oldEntry = oldIter.hasNext()? oldIter.next() : null;
                newEntry = newIter.hasNext() ? newIter.next() : null;
            } else if (cmp < 0) {
                updateVirtualMachineInterface(newVmiInfo);
                newEntry = newIter.hasNext() ? newIter.next() : null;
            } else {
                deleteVirtualMachineInterface(oldVmiInfo);
                oldEntry = oldIter.hasNext()? oldIter.next() : null;
            }
        }
        while (oldEntry != null) { 
            deleteVirtualMachineInterface(oldEntry.getValue());
            
            oldEntry = oldIter.hasNext()? oldIter.next() : null;
        }
        
        while (newEntry != null) {           
            updateVirtualMachineInterface(newEntry.getValue());
            newEntry = newIter.hasNext() ? newIter.next() : null;
        }
    }

    public static void updateVirtualNetwork(VmwareVirtualNetworkInfo vnInfo) 
            throws Exception {
        
        VmwareVirtualNetworkInfo oldVnInfo = MainDB.getVnByName(vnInfo.getName());
        updateVirtualNetwork(oldVnInfo, vnInfo);
    }
 
    private static void updateVirtualNetwork(
            VmwareVirtualNetworkInfo oldVnInfo,
            VmwareVirtualNetworkInfo newVnInfo) 
                    throws Exception {
        
        vmwareVNs.put(newVnInfo.getUuid(), newVnInfo);
        
        if (newVnInfo.apiVn == null && 
                oldVnInfo != null && oldVnInfo.apiVn != null) {
            newVnInfo.apiVn = oldVnInfo.apiVn;
        }

        if (oldVnInfo == null) {
            // create VN
            vncDB.updateVirtualNetwork(newVnInfo);
        }
    }

    public static void deleteVirtualNetwork(VmwareVirtualNetworkInfo vnInfo) 
            throws Exception {
        
        vncDB.deleteVirtualNetwork(vnInfo);
        
        String uuid = vnInfo.getUuid();
        if (vmwareVNs.containsKey(uuid)) {
            vmwareVNs.remove(uuid);
        }
    }

    public static void updateVirtualMachine(VmwareVirtualMachineInfo vmInfo)
            throws Exception {
        if (vmInfo == null) {
            return;
        }
        VmwareVirtualMachineInfo oldVmInfo = getVmById(vmInfo.getUuid());
        
        updateVirtualMachine(oldVmInfo, vmInfo);
    }
    
    private static void updateVirtualMachine(VmwareVirtualMachineInfo oldVmInfo, 
            VmwareVirtualMachineInfo newVmInfo) throws Exception {
        
        if (newVmInfo.apiVm == null && 
                oldVmInfo != null && oldVmInfo.apiVm != null) {
            newVmInfo.apiVm = oldVmInfo.apiVm;
        }
        
        vmwareVMs.put(newVmInfo.getUuid(), newVmInfo);
        
        if (oldVmInfo == null) {
            // create VM
            vncDB.updateVirtualMachine(newVmInfo);
        }
 
        syncVirtualMachineInterfaces(oldVmInfo, newVmInfo);
    }


    private static void updateVirtualMachineInterface(
            VmwareVirtualMachineInterfaceInfo vmiInfo) throws IOException {
        
        vncDB.updateVirtualMachineInterface(vmiInfo);
        
        if (vmiInfo.vnInfo.getExternalIpam() == false) {
            vncDB.updateInstanceIp(vmiInfo);
        }
        
        //VRouterNotifier.addPort(vmiInfo);
    }
 
    private static void deleteVirtualMachineInterface(VmwareVirtualMachineInterfaceInfo vmiInfo) 
            throws IOException {
        VRouterNotifier.deletePort(vmiInfo);
        
        if (vmiInfo.vnInfo.getExternalIpam() == false) {
            vncDB.deleteInstanceIp(vmiInfo);
        }
        vncDB.deleteVirtualMachineInterface(vmiInfo);
    }
    
    public static void deleteVirtualMachine(VmwareVirtualMachineInfo vmInfo)
            throws IOException {
        // loop through all the networks in which
        // this VM participates and delete VMIs and IP Instances
        for (Map.Entry<String, VmwareVirtualMachineInterfaceInfo> entry: 
                 vmInfo.getVmiInfo().entrySet()) {
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            
            //VRouterNotifier.deletePort(vmiInfo);
            
            VmwareVirtualNetworkInfo vnInfo = vmiInfo.getVnInfo();
             
            if (vnInfo.getExternalIpam() == false) {
                vncDB.deleteInstanceIp(vmiInfo);
            }
            vncDB.deleteVirtualMachineInterface(vmiInfo);
        }
        String uuid = vmInfo.getUuid();
        if (vmwareVMs.containsKey(uuid)) {
            vmwareVMs.remove(vmInfo.getUuid());
        }
    }

    public static void init(VCenterDB _vcenterDB, VncDB _vncDB, String mode) 
            throws Exception {
        vcenterDB = _vcenterDB;
        vncDB = _vncDB;
        
        vmwareVNs.clear();
        vmwareVMs.clear();
        
        SortedMap<String, VmwareVirtualNetworkInfo> newVNs = vcenterDB.readAllVirtualNetworks();
        if (mode == "vcenter-only") {
            SortedMap<String, VmwareVirtualNetworkInfo> oldVNs = vncDB.readVirtualNetworks();
            syncVirtualNetworks(oldVNs, newVNs);            
        } else {
            vmwareVNs = newVNs;
        }
        
        SortedMap<String, VmwareVirtualMachineInfo> newVMs = vcenterDB.readAllVirtualMachines();
        
        if (mode == "vcenter-only") {
            SortedMap<String, VmwareVirtualMachineInfo> oldVMs = vncDB.readVirtualMachines();
            syncVirtualMachines(oldVMs, newVMs);
        } else {
            vmwareVMs = newVMs;
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
