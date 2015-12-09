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
import org.apache.log4j.Logger;
import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualNetwork;

public class MainDB {
    private static volatile SortedMap<String, VmwareVirtualNetworkInfo> vmwareVNs =
            new ConcurrentSkipListMap<String, VmwareVirtualNetworkInfo>();
    private static volatile SortedMap<String, VmwareVirtualMachineInfo> vmwareVMs =
            new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
    
    private static volatile VncDB vncDB;
    private static volatile VCenterDB vcenterDB;
    private static volatile Mode mode;
    private final static Logger s_logger =
            Logger.getLogger(MainDB.class);
    
    public static SortedMap<String, VmwareVirtualNetworkInfo> getVNs() {
        return vmwareVNs;
    }

    public static SortedMap<String, VmwareVirtualMachineInfo> getVMs() {
        return vmwareVMs;
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
    
    public static void created(VmwareVirtualNetworkInfo vnInfo) {
        vmwareVNs.put(vnInfo.getUuid(), vnInfo);
    }
    
    public static void updated(VmwareVirtualNetworkInfo vnInfo) {
        if (!vmwareVNs.containsKey(vnInfo.getUuid())) {
            vmwareVNs.put(vnInfo.getUuid(), vnInfo);
        }
    }
    
    public static void deleted(VmwareVirtualNetworkInfo vnInfo) {
        if (vmwareVNs.containsKey(vnInfo.getUuid())) {
            vmwareVNs.remove(vnInfo.getUuid());
        }
    }
    
    public static void deleteVirtualNetwork(VmwareVirtualNetworkInfo vnInfo) {
        if (vmwareVNs.containsKey(vnInfo.getUuid())) {
            vmwareVNs.remove(vnInfo.getUuid());
        }
    }

    public static VmwareVirtualMachineInfo getVmById(String uuid) {
        if (vmwareVMs.containsKey(uuid)) {
            return vmwareVMs.get(uuid);
        }
        return null;
    }

    public static void created(VmwareVirtualMachineInfo vmInfo) {
        vmwareVMs.put(vmInfo.getUuid(), vmInfo);
    }
    
    public static void updated(VmwareVirtualMachineInfo vmInfo) {
        if (!vmwareVMs.containsKey(vmInfo.getUuid())) {
            vmwareVMs.put(vmInfo.getUuid(), vmInfo);
        }
    }
    
    public static void deleted(VmwareVirtualMachineInfo vmInfo) {
        if (vmwareVMs.containsKey(vmInfo.getUuid())) {
            vmwareVNs.remove(vmInfo.getUuid());
        }
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
                    newEntry.getValue().sync(oldEntry.getValue(), vncDB);
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else if (cmp < 0) {
                    newEntry.getValue().create(vncDB);
                    newEntry = newIter.hasNext()? newIter.next() : null;
                } else { 
                    if (mode != Mode.VCENTER_AS_COMPUTE) {
                        oldEntry.getValue().delete(vncDB);
                    }
                    oldEntry = oldIter.hasNext()? oldIter.next() : null;
                }
            } catch (Exception e) {
                //s_logger.error("Cannot sync " + entry1.getKey());
            }
        }

        if (mode != Mode.VCENTER_AS_COMPUTE) {
            while (oldEntry != null) {
                try {
                    oldEntry.getValue().delete(vncDB);
                } catch (Exception e) {
                    //s_logger.error("Cannot delete " + entry2.getKey());
                }
                oldEntry = oldIter.hasNext()? oldIter.next() : null;
            }
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
  
    public static <K extends Comparable<K>, V extends VCenterObject> 
    void update(SortedMap<K, V> oldMap, SortedMap<K, V> newMap) {
        
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
    
    public static void sync(VCenterDB _vcenterDB, VncDB _vncDB, Mode _mode) 
            throws Exception {
        vcenterDB = _vcenterDB;
        vncDB = _vncDB;
        mode = _mode;
        
        vmwareVNs.clear();
        vmwareVMs.clear();
        
        vmwareVNs = vcenterDB.readVirtualNetworks();
        SortedMap<String, VmwareVirtualNetworkInfo> oldVNs = vncDB.readVirtualNetworks();
        sync(oldVNs, vmwareVNs);
        
        vmwareVMs = vcenterDB.readVirtualMachines();
        SortedMap<String, VmwareVirtualMachineInfo> oldVMs = vncDB.readVirtualMachines();
        sync(oldVMs, vmwareVMs);
         
        printInfo();
        
        s_logger.info("\nSync complete, waiting for events\n");
    }

    private static void printInfo() {
        System.out.println("\nNetworks after sync:");
        for (VmwareVirtualNetworkInfo vnInfo: vmwareVNs.values()) {
            System.out.println(vnInfo.toStringBuffer());
        }
        
        System.out.println("\nVirtual Machines after sync:");
        for (VmwareVirtualMachineInfo vmInfo: vmwareVMs.values()) {
            System.out.println(vmInfo.toStringBuffer());
        }
        System.out.println("\n");
    }
}
