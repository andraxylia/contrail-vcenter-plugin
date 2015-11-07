package net.juniper.contrail.vcenter;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import com.vmware.vim25.DVPortSetting;
import com.vmware.vim25.DVSConfigInfo;
import com.vmware.vim25.DistributedVirtualSwitchKeyedOpaqueBlob;
import com.vmware.vim25.Event;
import com.vmware.vim25.IpPool;
import com.vmware.vim25.IpPoolIpPoolConfigInfo;
import com.vmware.vim25.VMwareDVSConfigInfo;
import com.vmware.vim25.VMwareDVSPortSetting;
import com.vmware.vim25.VMwareDVSPvlanMapEntry;
import com.vmware.vim25.VmwareDistributedVirtualSwitchPvlanSpec;
import com.vmware.vim25.VmwareDistributedVirtualSwitchVlanIdSpec;
import com.vmware.vim25.VmwareDistributedVirtualSwitchVlanSpec;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.DistributedVirtualPortgroup;
import com.vmware.vim25.mo.ManagedObject;
import com.vmware.vim25.mo.VmwareDistributedVirtualSwitch;
import com.vmware.vim25.mo.util.PropertyCollectorUtil;

import net.juniper.contrail.api.types.VirtualNetwork;

public class VmwareVirtualNetworkInfo {
    private String uuid; // required attribute, key for this object
    private String name;
    private short isolatedVlanId;
    private short primaryVlanId;
    private SortedMap<String, VmwareVirtualMachineInfo> vmInfo;
    private String subnetAddress;
    private String subnetMask;
    private String gatewayAddress;
    private boolean ipPoolEnabled;
    private String range;
    private boolean externalIpam;
    
    // Vmware
    com.vmware.vim25.mo.Network net;
    DistributedVirtualPortgroup dpg;
    DVPortSetting portSetting;
    com.vmware.vim25.mo.VmwareDistributedVirtualSwitch dvs;
    String dvsName;
    com.vmware.vim25.mo.Datacenter dc;
    String dcName;

    // API server
    net.juniper.contrail.api.types.VirtualNetwork apiVn;

    public VmwareVirtualNetworkInfo(String name, short isolatedVlanId,
            short primaryVlanId, SortedMap<String, VmwareVirtualMachineInfo> vmInfo,
            String subnetAddress, String subnetMask, String gatewayAddress,
            boolean ipPoolEnabled, String range, boolean externalIpam) {
        this.name = name;
        this.isolatedVlanId = isolatedVlanId;
        this.primaryVlanId = primaryVlanId;
        this.vmInfo = vmInfo;
        this.subnetAddress = subnetAddress;
        this.subnetMask = subnetMask;
        this.gatewayAddress = gatewayAddress;
        this.ipPoolEnabled = ipPoolEnabled;
        this.range = range;
        this.externalIpam = externalIpam;
        vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
    }

    public VmwareVirtualNetworkInfo(String uuid) {
        this.uuid = uuid;
        vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
    }

    public VmwareVirtualNetworkInfo(Event event,  VCenterDB vcenterDB) throws Exception {
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
            name = event.getNet().getName();
            net = vcenterDB.getVmwareNetwork(name, dvs, dvsName, dcName);
        }
        
        // Extract IP Pools
        IpPool[] ipPools = vcenterDB.getIpPoolManager().queryIpPools(dc);
        if (ipPools == null || ipPools.length == 0) {
            throw new Exception(" IP Pools NOT configured");
        }
    
        // Extract private vlan entries for the virtual switch
        DVSConfigInfo dvsConf = dvs.getConfig();
        
        if (dvsConf == null) {
            throw new Exception("dvSwitch: " + dvsName +
                    " Datacenter: " + dcName + " ConfigInfo " +
                    "is empty");
        }
    
        if (!(dvsConf instanceof VMwareDVSConfigInfo)) {
            throw new Exception("dvSwitch: " + dvsName +
                    " Datacenter: " + dcName + " ConfigInfo " +
                    "isn't instanceof VMwareDVSConfigInfo");
        }
        VMwareDVSConfigInfo dvsConfigInfo = (VMwareDVSConfigInfo) dvsConf;
        
        VMwareDVSPvlanMapEntry[] pvlanMapArray = dvsConfigInfo.getPvlanConfig();
        if (pvlanMapArray == null) {
            throw new Exception("dvSwitch: " + dvsName +
                    " Datacenter: " + dcName + " Private VLAN NOT" +
                    "configured");
        }

        dpg = vcenterDB.getVmwareDpg(name, dvs, dvsName, dcName);
        ManagedObject mo[] = new ManagedObject[1];
        mo[0] = dpg; 
 
        Hashtable[] pTables = PropertyCollectorUtil.retrieveProperties(mo,
                                "DistributedVirtualPortgroup",
                new String[] {"name",
                "config.key",
                "config.defaultPortConfig",
                "config.vendorSpecificConfig",
                "summary.ipPoolId",
                "summary.ipPoolName",
                });
    
        if (pTables == null || pTables[0] == null) {
            throw new RemoteException("Could not read properties for network " + name);
        }
        
        populateInfo(vcenterDB, pTables[0], ipPools, pvlanMapArray);
    }

    public VmwareVirtualNetworkInfo(VCenterDB vcenterDB,
            DistributedVirtualPortgroup dpg, Hashtable pTable,
            com.vmware.vim25.mo.Datacenter dc, String dcName,
            com.vmware.vim25.mo.VmwareDistributedVirtualSwitch dvs,
            String dvsName,
            IpPool[] ipPools,
            VMwareDVSPvlanMapEntry[] pvlanMapArray) throws Exception {
               
        if (vcenterDB == null || dpg == null || pTable == null
                || dvs == null || dvsName == null
                || dc == null || dcName == null
                || ipPools == null || pvlanMapArray == null) {
            throw new IllegalArgumentException();
        }
        
        this.dpg = dpg;
        this.dvs = dvs;
        this.dvsName = dvsName;
        
        populateInfo(vcenterDB, pTable, ipPools, pvlanMapArray);
    }
    
    void populateInfo(VCenterDB vcenterDB,
            Hashtable pTable,
            IpPool[] ipPools,
            VMwareDVSPvlanMapEntry[] pvlanMapArray) throws Exception {
        
        // Extract dvPg configuration info and port setting
        portSetting = (DVPortSetting) pTable.get("config.defaultPortConfig");

        if (ignore()) {
            return;
        }

        name = (String) pTable.get("name");

        String key = (String) pTable.get("config.key");
        byte[] vnKeyBytes = key.getBytes();
        String vnUuid = UUID.nameUUIDFromBytes(vnKeyBytes).toString();

        // get pvlan/vlan info for the portgroup.
        HashMap<String, Short> vlan = vcenterDB.getVlanInfo(dpg, portSetting, pvlanMapArray);
        if (vlan == null) {
            return;
        }

        primaryVlanId = vlan.get("primary-vlan");
        isolatedVlanId = vlan.get("secondary-vlan");
        
        // Find associated IP Pool
        Integer poolId     = (Integer) pTable.get("summary.ipPoolId");
        IpPool ipPool = vcenterDB.getIpPool(dpg, name, ipPools, poolId);
        if (ipPool == null) {
            return;
        } else {
            IpPoolIpPoolConfigInfo ipConfigInfo = ipPool.getIpv4Config();
    
            // ifconfig setting
            subnetAddress = ipConfigInfo.getSubnetAddress();
            subnetMask = ipConfigInfo.getNetmask();
            gatewayAddress = ipConfigInfo.getGateway();
            ipPoolEnabled = ipConfigInfo.getIpPoolEnabled();
            range = ipConfigInfo.getRange();
        }

        // Read externalIpam flag from custom field
        DistributedVirtualSwitchKeyedOpaqueBlob[] opaqueBlobs = null;
        Object obj = pTable.get("config.vendorSpecificConfig");
        if (obj instanceof DistributedVirtualSwitchKeyedOpaqueBlob[]) {
            opaqueBlobs = (DistributedVirtualSwitchKeyedOpaqueBlob[]) obj;
        }
        externalIpam = vcenterDB.getExternalIpamInfo(opaqueBlobs, name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public short getIsolatedVlanId() {
        return isolatedVlanId;
    }

    public void setIsolatedVlanId(short vlanId) {
        this.isolatedVlanId = vlanId;
    }

    public short getPrimaryVlanId() {
        return primaryVlanId;
    }

    public void setPrimaryVlanId(short vlanId) {
        this.primaryVlanId = vlanId;
    }

    public boolean getIpPoolEnabled() {
        return ipPoolEnabled;
    }

    public void setIpPoolEnabled(boolean _ipPoolEnabled) {
        this.ipPoolEnabled = _ipPoolEnabled;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String _range) {
        this.range = _range;
    }

    public SortedMap<String, VmwareVirtualMachineInfo> getVmInfo() {
        return vmInfo;
    }

    public void setVmInfo(SortedMap<String, VmwareVirtualMachineInfo> vmInfo) {
        this.vmInfo = vmInfo;
    }

    public String getSubnetAddress() {
        return subnetAddress;
    }

    public void setSubnetAddress(String subnetAddress) {
        this.subnetAddress = subnetAddress;
    }

    public String getSubnetMask() {
        return subnetMask;
    }

    public void setSubnetMask(String subnetMask) {
        this.subnetMask = subnetMask;
    }

    public String getGatewayAddress() {
        return gatewayAddress;
    }

    public void setGatewayAddress(String gatewayAddress) {
        this.gatewayAddress = gatewayAddress;
    }

    public boolean getExternalIpam() {
        return externalIpam;
    }

    public void setExternalIpam(boolean externalIpam) {
        this.externalIpam = externalIpam;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public DistributedVirtualPortgroup getDpg() {
        return dpg;
    }
    
    public void setDpg(DistributedVirtualPortgroup dpg) {
        this.dpg = dpg;
    }
    
    public boolean equals(VmwareVirtualNetworkInfo vn) {
        if (vn == null) {
            return false;
        }
        if (name != null && !name.equals(vn.name)
                || name == null && vn.name != null) {
            return false;
        }
        if (uuid != null && !uuid.equals(vn.uuid)
                || uuid == null && vn.uuid != null) {
            return false;
        }
        if (isolatedVlanId != vn.isolatedVlanId
                || primaryVlanId != vn.primaryVlanId
                || ipPoolEnabled != vn.ipPoolEnabled
                || externalIpam != vn.externalIpam) {
            return false;
        }
        if (subnetAddress != null && !subnetAddress.equals(vn.subnetAddress)
                || subnetAddress == null && vn.subnetAddress != null) {
            return false;
        }
        if (subnetMask != null && !subnetMask.equals(vn.subnetMask)
                || subnetMask == null && vn.subnetMask != null) {
            return false;
        }
        if (gatewayAddress != null && !gatewayAddress.equals(vn.gatewayAddress)
                || gatewayAddress == null && vn.gatewayAddress != null) {
            return false;
        }
        if (range != null && !range.equals(vn.range)
                || range == null && vn.range != null) {
            return false;
        }
        return true;
    }
    
    public String toString() {
        return "VN <" + name + ", " + uuid + ">";
    }
    
    public StringBuffer toStringBuffer() {
        StringBuffer s = new StringBuffer(
                "VN <" + name + ", " + uuid + ">\n");
        Iterator<Entry<String, VmwareVirtualMachineInfo>> iter =
                vmInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualMachineInfo> entry = iter.next();
            s.append(entry.getValue().toString());
        }
        return s;
    }
    
    boolean ignore() {
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
}
