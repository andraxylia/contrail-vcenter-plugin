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
    static volatile SortedMap<String, VmwareVirtualNetworkInfo> vmwareVNs =
            new ConcurrentSkipListMap<String, VmwareVirtualNetworkInfo>();
    static volatile SortedMap<String, VmwareVirtualMachineInfo> vmwareVMs =
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

    public static <K extends Comparable<K>, V extends VCenterObject> 
    void sync(SortedMap<K, V> oldMap, SortedMap<K, V> newMap) {
        
        Iterator<Entry<K, V>> oldIter = oldMap.entrySet().iterator();
        Entry<K, V> oldEntry = oldIter.hasNext()? oldIter.next() : null;
        Iterator<Entry<K, V>> newIter = newMap.entrySet().iterator();
        Entry<K, V> newEntry = newIter.hasNext()? newIter.next() : null;
        
        while (oldEntry != null && newEntry != null) {
            Integer cmp = newEntry.getKey().compareTo(oldEntry.getKey());
            try {
                if (cmp == 0) {
                    oldEntry.getValue().update(newEntry.getValue(), vncDB);
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else if (cmp < 0) {
                    newEntry.getValue().create(vncDB);
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else {
                    oldEntry.getValue().delete(vncDB);
                    oldIter.remove();
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                }
            } catch (Exception e) {
                //s_logger.error("Cannot sync " + entry1.getKey());
            }
        }

        while (oldEntry != null) {
            try {
                oldEntry.getValue().delete(vncDB);
            } catch (Exception e) {
                //s_logger.error("Cannot delete " + entry2.getKey());
            }
            oldEntry = oldIter.hasNext()? oldIter.next() : null;
        }
        
        while (newEntry != null) {
            try {
                newEntry.getValue().create(vncDB);
            } catch (Exception e) {
                //s_logger.error("Cannot create VN " + entry1.getKey());
            }
            newEntry = newIter.hasNext()? newIter.next() : null;
        }
    }

    

    public static void init(VCenterDB _vcenterDB, VncDB _vncDB, String mode) 
            throws Exception {
        vcenterDB = _vcenterDB;
        vncDB = _vncDB;
        
        vmwareVNs.clear();
        vmwareVMs.clear();
        
        vmwareVNs = vncDB.readVirtualNetworks();
        
        if (mode == "vcenter-only") {
            SortedMap<String, VmwareVirtualNetworkInfo> newVNs = vcenterDB.readVirtualNetworks();
            sync(vmwareVNs, newVNs);            
        }
        
        printInfo();
        
        vmwareVMs = vncDB.readVirtualMachines();
      
        if (mode == "vcenter-only") {
            SortedMap<String, VmwareVirtualMachineInfo> newVMs = vcenterDB.readVirtualMachines();
            sync(vmwareVMs, newVMs);
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
