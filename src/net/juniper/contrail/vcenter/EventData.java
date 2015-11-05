/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */

package net.juniper.contrail.vcenter;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.vmware.vim25.DVPortSetting;
import com.vmware.vim25.DistributedVirtualSwitchKeyedOpaqueBlob;
import com.vmware.vim25.Event;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.GuestNicInfo;
import com.vmware.vim25.IpPool;
import com.vmware.vim25.IpPoolIpPoolConfigInfo;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.NetIpConfigInfo;
import com.vmware.vim25.NetIpConfigInfoIpAddress;
import com.vmware.vim25.VMwareDVSConfigInfo;
import com.vmware.vim25.VMwareDVSPvlanMapEntry;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.VmDasBeingResetEventReasonCode;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Datastore;
import com.vmware.vim25.mo.VirtualMachine;
import com.vmware.vim25.mo.VmwareDistributedVirtualSwitch;
import com.vmware.vim25.mo.util.PropertyCollectorUtil;
import com.vmware.vim25.mo.DistributedVirtualPortgroup;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.Network;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.IpPoolManager;
import com.vmware.vim25.mo.ManagedObject;

public class EventData {
    VCenterDB vcenterDB;
    VncDB vncDB;
    boolean changed;
    boolean updateVrouterNeeded;

    //VCenter objects
    // using fully qualified name because of collisions
    com.vmware.vim25.Event event;
    com.vmware.vim25.mo.Datacenter dc;
    String dcName;
    com.vmware.vim25.mo.Datastore ds;
    String dsName;
    com.vmware.vim25.mo.VmwareDistributedVirtualSwitch dvs;
    String dvsName;
    com.vmware.vim25.mo.DistributedVirtualPortgroup dpg;
    String dpgName;
    com.vmware.vim25.mo.Network nw;
    String nwName;
    com.vmware.vim25.mo.HostSystem host;
    String hostName;
    com.vmware.vim25.mo.VirtualMachine vm; //this is the vmwareVM
    String vmName;

    // Cached objects
    VmwareVirtualNetworkInfo vnInfo; //this is our cached VN, names are messed up
    VmwareVirtualMachineInfo vmInfo; //this is our cached VM, names are messed up
    String vrouterIpAddress;

    //API server objects
    net.juniper.contrail.api.types.VirtualNetwork apiVn;
    net.juniper.contrail.api.types.VirtualMachine apiVm;
    net.juniper.contrail.api.types.VirtualMachineInterface apiVmi;
    net.juniper.contrail.api.types.InstanceIp apiInstanceIp;

    EventData(Event event,  VCenterDB vcenterDB, VncDB vncDB) throws Exception {
        this.event = event;
        this.vcenterDB = vcenterDB;
        this.vncDB = vncDB;

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

        if (event.getNet() != null) {
            nwName = event.getNet().getName();
            nw = vcenterDB.getVmwareNetwork(nwName, dvs, dvsName, dcName);

            dpgName = event.getNet().getName();
            dpg = vcenterDB.getVmwareDpg(dpgName, dvs, dvsName, dcName);

            vnInfo = vcenterDB.createVnInfo(this);
        }

        if (event.getHost() != null) {
            hostName = event.getHost().getName();
            host = vcenterDB.getVmwareHost(hostName, dc, dcName);

            vrouterIpAddress = vcenterDB.getVRouterVMIpFabricAddress(
                    VCenterDB.contrailVRouterVmNamePrefix,
                    host, hostName);

            if (event.getVm() != null) {
                vmName = event.getVm().getName();
  
                vm = vcenterDB.getVmwareVirtualMachine(vmName, host, hostName, dcName);

                vmInfo = vcenterDB.createVmInfo(this);              
             }
        }
    }
}
