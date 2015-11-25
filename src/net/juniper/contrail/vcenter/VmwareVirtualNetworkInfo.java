package net.juniper.contrail.vcenter;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
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
import com.vmware.vim25.RuntimeFault;
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

public class VmwareVirtualNetworkInfo extends VCenterObject {
    private String uuid; // required attribute, key for this object
    private String name;
    private short isolatedVlanId;
    private short primaryVlanId;
    private SortedMap<String, VmwareVirtualMachineInfo> vmInfo;
    private SortedMap<String, VmwareVirtualMachineInterfaceInfo> vmiInfoMap; // key is MAC address
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
        this.vmInfo = vmInfo;
    }

    public VmwareVirtualNetworkInfo(net.juniper.contrail.api.types.VirtualNetwork vn) {
        this.apiVn = vn;
        this.uuid = vn.getUuid();
        name = vn.getName();
        this.vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
        this.vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
        this.externalIpam = vn.getExternalIpam();
    }
    
    public VmwareVirtualNetworkInfo(Event event,  VCenterDB vcenterDB) throws Exception {
        vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
        
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
        
        populateInfo(vcenterDB, pTables[0]);
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
        vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
        vmiInfoMap = new ConcurrentSkipListMap<String, VmwareVirtualMachineInterfaceInfo>();
        this.dc = dc;
        this.dcName = dcName;
        this.dpg = dpg;
        this.dvs = dvs;
        this.dvsName = dvsName;
        
        populateInfo(vcenterDB, pTable);
    }
    
    void populateInfo(VCenterDB vcenterDB,
            Hashtable pTable) throws Exception {
        
        vmInfo = new ConcurrentSkipListMap<String, VmwareVirtualMachineInfo>();
        
        // Extract dvPg configuration info and port setting
        portSetting = (DVPortSetting) pTable.get("config.defaultPortConfig");

        if (ignore()) {
            return;
        }

        name = (String) pTable.get("name");

        String key = (String) pTable.get("config.key");
        byte[] vnKeyBytes = key.getBytes();
        uuid = UUID.nameUUIDFromBytes(vnKeyBytes).toString();

        populateVlans(vcenterDB);
        
        populateAddressManagement(vcenterDB, pTable);
    }

    private void populateAddressManagement(VCenterDB vcenterDB, Hashtable pTable)
            throws RuntimeFault, RemoteException, Exception {
        // Find associated IP Pool
        // Extract IP Pools
        IpPool[] ipPools = vcenterDB.getIpPoolManager().queryIpPools(dc);
        if (ipPools == null || ipPools.length == 0) {
            throw new Exception(" IP Pools NOT configured");
        }
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

    private void populateVlans(VCenterDB vcenterDB) throws Exception {
        // get pvlan/vlan info for the portgroup.
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
        
        HashMap<String, Short> vlan = vcenterDB.getVlanInfo(dpg, portSetting, pvlanMapArray);
        if (vlan == null) {
            return;
        }

        primaryVlanId = vlan.get("primary-vlan");
        isolatedVlanId = vlan.get("secondary-vlan");
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
    
    public void created(VmwareVirtualMachineInterfaceInfo vmiInfo) {
        vmiInfoMap.put(vmiInfo.getMacAddress(), vmiInfo);
    }
    
    public void updated(VmwareVirtualMachineInterfaceInfo vmiInfo) {
        if (!vmiInfoMap.containsKey(vmiInfo.getMacAddress())) {
            vmiInfoMap.put(vmiInfo.getMacAddress(), vmiInfo);
        }
    }

    public void deleted(VmwareVirtualMachineInterfaceInfo vmiInfo) {
        if (vmiInfoMap.containsKey(vmiInfo.getMacAddress())) {
            vmiInfoMap.remove(vmiInfo.getMacAddress());
        }
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
        
        for (Map.Entry<String, VmwareVirtualMachineInterfaceInfo> entry: 
            vmiInfoMap.entrySet()) {
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            s.append(vmiInfo.toString());
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
    
    // change the method below to use Observer pattern and get rid of the Vnc param
    @Override
    void create(VncDB vncDB) throws Exception {
        
        if (ignore()) {
            return;
        }
       
        vncDB.createVirtualNetwork(this);
        MainDB.created(this);
    }
    
    @Override
    void update(VCenterObject obj, VncDB vncDB) 
                    throws Exception {
        
        if (ignore()) {
            return;
        }
        
        VmwareVirtualNetworkInfo newVnInfo = (VmwareVirtualNetworkInfo)obj;
        
        if (newVnInfo.name != null) {
            name = newVnInfo.name;
        }
        if (newVnInfo.isolatedVlanId != 0) {
            isolatedVlanId = newVnInfo.isolatedVlanId;
        }
        
        if (newVnInfo.primaryVlanId != 0) {
            primaryVlanId = newVnInfo.primaryVlanId;
        }
        
        if (newVnInfo.subnetAddress != null) {
            subnetAddress = newVnInfo.subnetAddress;
        }
        
        if (newVnInfo.subnetMask != null) {
            subnetAddress = newVnInfo.subnetMask;
        }
        
        if (newVnInfo.gatewayAddress != null) {
            gatewayAddress = newVnInfo.gatewayAddress;
        }
        if (newVnInfo.ipPoolEnabled != false) {
            ipPoolEnabled = newVnInfo.ipPoolEnabled;
        }
        
        if (newVnInfo.range != null) {
            range = newVnInfo.range;
        }
        if (newVnInfo.externalIpam != false) {
            externalIpam = newVnInfo.externalIpam;
        }
        
        if (newVnInfo.net != null) {
            net = newVnInfo.net;
        }
        if (newVnInfo.dpg != null) {
            dpg = newVnInfo.dpg;
        }
        if (newVnInfo.portSetting != null) {
            portSetting = newVnInfo.portSetting;
        }
        if (newVnInfo.dvs != null) {
            dvs = newVnInfo.dvs;
        }
        
        if (newVnInfo.dvsName != null) {
            dvsName = newVnInfo.dvsName;
        }
        
        // notify observers
        // for networks we do not update the API server
    }
    
    @Override
    void sync(VCenterObject obj, VncDB vncDB) 
                    throws Exception {
        
        VmwareVirtualNetworkInfo oldVnInfo = (VmwareVirtualNetworkInfo)obj;
        
        if (apiVn == null && oldVnInfo.apiVn != null) {
            apiVn = oldVnInfo.apiVn;
        }
        
        // notify observers
        // for networks we do not update the API server
        
    }
    
    @Override
    void delete(VncDB vncDB) 
            throws Exception {
                      
        for (Map.Entry<String, VmwareVirtualMachineInterfaceInfo> entry: 
            vmiInfoMap.entrySet()) {
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            vmiInfo.delete(vncDB);
        }
        
        vncDB.deleteVirtualNetwork(this);
        
        MainDB.deleted(this);
    }
}
