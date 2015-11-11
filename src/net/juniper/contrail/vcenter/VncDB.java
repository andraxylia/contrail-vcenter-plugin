/**
 * Copyright (c) 2014 Juniper Networks, Inc. All rights reserved.
 */

package net.juniper.contrail.vcenter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.commons.net.util.SubnetUtils;

import net.juniper.contrail.api.ApiConnector;
import net.juniper.contrail.api.ApiConnectorFactory;
import net.juniper.contrail.api.ApiPropertyBase;
import net.juniper.contrail.api.ObjectReference;
import net.juniper.contrail.api.types.InstanceIp;
import net.juniper.contrail.api.types.FloatingIp;
import net.juniper.contrail.api.types.MacAddressesType;
import net.juniper.contrail.api.types.NetworkIpam;
import net.juniper.contrail.api.types.SecurityGroup;
import net.juniper.contrail.api.types.PolicyEntriesType;
import net.juniper.contrail.api.types.SubnetType;
import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualMachineInterface;
import net.juniper.contrail.api.types.VirtualNetwork;
import net.juniper.contrail.api.types.VnSubnetsType;
import net.juniper.contrail.api.types.Project;
import net.juniper.contrail.api.types.IdPermsType;
import net.juniper.contrail.contrail_vrouter_api.ContrailVRouterApi;

import com.google.common.base.Throwables;

public class VncDB {
    private static final Logger s_logger = 
            Logger.getLogger(VncDB.class);
    protected static final int vrouterApiPort = 9090;
    protected final String apiServerAddress;
    protected final int apiServerPort;
    private final String mode;
    protected HashMap<String, ContrailVRouterApi> vrouterApiMap;
    
    protected volatile ApiConnector apiConnector;
    private boolean alive;
    private Project vCenterProject;
    private NetworkIpam vCenterIpam;
    private SecurityGroup vCenterDefSecGrp;
    private IdPermsType vCenterIdPerms;

    public static final String VNC_ROOT_DOMAIN     = "default-domain";
    public static final String VNC_VCENTER_PROJECT = "vCenter";
    public static final String VNC_VCENTER_IPAM    = "vCenter-ipam";
    public static final String VNC_VCENTER_DEFAULT_SG    = "default";
    public static final String VNC_VCENTER_PLUGIN  = "vcenter-plugin";
    public static final String VNC_VCENTER_TEST_PROJECT = "vCenter-test";
    public static final String VNC_VCENTER_TEST_IPAM    = "vCenter-ipam-test";
    
    public VncDB(String apiServerAddress, int apiServerPort, String mode) {
        this.apiServerAddress = apiServerAddress;
        this.apiServerPort = apiServerPort;
        this.mode = mode;

        // Create vrouter api map
        vrouterApiMap = new HashMap<String, ContrailVRouterApi>();

        // Create global id-perms object.
        vCenterIdPerms = new IdPermsType();
        vCenterIdPerms.setCreator("vcenter-plugin");
        vCenterIdPerms.setEnable(true);

    }
    
    public void setApiConnector(ApiConnector _apiConnector) {
        apiConnector = _apiConnector;
    }

    public ApiConnector getApiConnector() {
        return apiConnector;
    }

    public String getApiServerAddress() {
        return apiServerAddress;
    }
    
    public int getApiServerPort() {
        return apiServerPort;
    }
    
    public HashMap<String, ContrailVRouterApi>  getVRouterApiMap() {
        return vrouterApiMap;
    }

    public IdPermsType getVCenterIdPerms() {
        return vCenterIdPerms;
    }

    public Project getVCenterProject() {
        return vCenterProject;
    }

    public boolean isServerAlive() {
        return alive;
    }
    
    public boolean isVncApiServerAlive() {
        if (apiConnector == null) {
            apiConnector = ApiConnectorFactory.build(apiServerAddress,
                                                     apiServerPort);
            if (apiConnector == null) {
                s_logger.error(" failed to create ApiConnector.. retry later");
                alive = false;
                return false;
            }
        }

        // Read project list as a life check
        s_logger.info(" Checking if api-server is alive and kicking..");

        try {
            List<Project> projects = (List<Project>) apiConnector.list(Project.class, null);
            if (projects == null) {
                s_logger.error(" ApiServer not fully awake yet.. retry again..");
                alive = false;
                return false;
            }
        } catch (IOException e) {
            alive = false;
            return false;
        }

        alive = true;
        s_logger.info(" Api-server alive. Got the pulse..");
        return true;

    }

    public boolean Initialize() {

        // Check if api-server is alive
        if (isVncApiServerAlive() == false)
            return false;

        // Check if Vmware Project exists on VNC. If not, create one.
        try {
            vCenterProject = (Project) apiConnector.findByFQN(Project.class, 
                                        VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT);
        } catch (Exception e) {
            s_logger.error("Exception : " + e);
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error(stackTrace);
            return false;
        }
        s_logger.info(" fqn-to-uuid complete..");
        if (vCenterProject == null) {
            s_logger.info(" vCenter project not present, creating ");
            vCenterProject = new Project();
            vCenterProject.setName("vCenter");
            vCenterProject.setIdPerms(vCenterIdPerms);
            try {
                if (!apiConnector.create(vCenterProject)) {
                    s_logger.error("Unable to create project: " + vCenterProject.getName());
                    return false;
                }
            } catch (Exception e) {
                s_logger.error("Exception : " + e);
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
                return false;
            }
        } else {
            s_logger.info(" vCenter project present, continue ");
        }

        // Check if VMWare vCenter-ipam exists on VNC. If not, create one.
        try {
            vCenterIpam = (NetworkIpam) apiConnector.findByFQN(NetworkIpam.class,
                       VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + VNC_VCENTER_IPAM);
        } catch (Exception e) {
            s_logger.error("Exception : " + e);
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error(stackTrace);
            return false;
        }

        if (vCenterIpam == null) {
            s_logger.info(" vCenter Ipam not present, creating ...");
            vCenterIpam = new NetworkIpam();
            vCenterIpam.setParent(vCenterProject);
            vCenterIpam.setName("vCenter-ipam");
            vCenterIpam.setIdPerms(vCenterIdPerms);
            try {
                if (!apiConnector.create(vCenterIpam)) {
                    s_logger.error("Unable to create Ipam: " + vCenterIpam.getName());
                }
            } catch (Exception e) {
                s_logger.error("Exception : " + e);
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
                return false;
            }
        } else {
            s_logger.info(" vCenter Ipam present, continue ");
        }

        // Check if VMWare vCenter default security-group exists on VNC. If not, create one.
        try {
            vCenterDefSecGrp = (SecurityGroup) apiConnector.findByFQN(SecurityGroup.class,
                       VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + VNC_VCENTER_DEFAULT_SG);
        } catch (Exception e) {
            s_logger.error("Exception : " + e);
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error(stackTrace);
            return false;
        }

        if (vCenterDefSecGrp == null) {
            s_logger.info(" vCenter default Security-group not present, creating ...");
            vCenterDefSecGrp = new SecurityGroup();
            vCenterDefSecGrp.setParent(vCenterProject);
            vCenterDefSecGrp.setName("default");
            vCenterDefSecGrp.setIdPerms(vCenterIdPerms);

            PolicyEntriesType sg_rules = new PolicyEntriesType();

            PolicyEntriesType.PolicyRuleType ingress_rule = 
                              new PolicyEntriesType.PolicyRuleType(
                                      null,
                                      UUID.randomUUID().toString(),
                                      ">",
                                      "any",
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + "default", null)}),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //src_ports
                                       null, //application
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, "local", null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //dest_ports
                                       null, // action_list
                                       "IPv4"); // ethertype
            sg_rules.addPolicyRule(ingress_rule);

            PolicyEntriesType.PolicyRuleType egress_rule  = 
                              new PolicyEntriesType.PolicyRuleType(
                                      null,
                                      UUID.randomUUID().toString(),
                                      ">",
                                      "any",
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, "local", null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //src_ports
                                       null, //application
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(new SubnetType("0.0.0.0", 0), null, null, null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //dest_ports
                                       null, // action_list
                                       "IPv4"); // ethertype);
            sg_rules.addPolicyRule(egress_rule);

            vCenterDefSecGrp.setEntries(sg_rules);

            try {
                if (!apiConnector.create(vCenterDefSecGrp)) {
                    s_logger.error("Unable to create defSecGrp: " + vCenterDefSecGrp.getName());
                }
            } catch (Exception e) {
                s_logger.error("Exception : " + e);
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
                return false;
            }
        } else {
            s_logger.info(" vCenter default sec-group present, continue ");
        }


        return true;
    }

 public boolean TestInitialize() {

        // Check if Vmware Test Project exists on VNC. If not, create one.
        try {
            vCenterProject = (Project) apiConnector.findByFQN(Project.class, 
                                        VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_TEST_PROJECT);
        } catch (IOException e) {
            return false;
        }
        if (vCenterProject == null) {
            s_logger.info(" vCenter-test project not present, creating ");
            vCenterProject = new Project();
            vCenterProject.setName("vCenter-test");
            try {
                if (!apiConnector.create(vCenterProject)) {
                    s_logger.error("Unable to create project: " + vCenterProject.getName());
                    return false;
                }
            } catch (IOException e) { 
                s_logger.error("Exception : " + e);
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
                return false;
            }
        } else {
            s_logger.info(" vCenter-test project present, continue ");
        }

        // Check if VMWare vCenter-test ipam exists on VNC. If not, create one.
        try {
            vCenterIpam = (NetworkIpam) apiConnector.findByFQN(NetworkIpam.class,
                       VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + VNC_VCENTER_TEST_IPAM);
        } catch (IOException e) {
            return false;
        }

        if (vCenterIpam == null) {
            s_logger.info(" vCenter test Ipam not present, creating ...");
            vCenterIpam = new NetworkIpam();
            vCenterIpam.setParent(vCenterProject);
            vCenterIpam.setName("vCenter-ipam-test");
            try {
                if (!apiConnector.create(vCenterIpam)) {
                    s_logger.error("Unable to create test Ipam: " + vCenterIpam.getName());
                }
            } catch (IOException e) { 
                s_logger.error("Exception : " + e);
                e.printStackTrace();
                return false;
            }
        } else {
            s_logger.info(" vCenter test Ipam present, continue ");
        }

        // Check if VMWare vCenter default security-group exists on VNC. If not, create one.
        try {
            vCenterDefSecGrp = (SecurityGroup) apiConnector.findByFQN(SecurityGroup.class,
                       VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + VNC_VCENTER_DEFAULT_SG);
        } catch (Exception e) {
            s_logger.error("Exception : " + e);
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error(stackTrace);
            return false;
        }

        if (vCenterDefSecGrp == null) {
            s_logger.info(" vCenter default Security-group not present, creating ...");
            vCenterDefSecGrp = new SecurityGroup();
            vCenterDefSecGrp.setParent(vCenterProject);
            vCenterDefSecGrp.setName("default");

            PolicyEntriesType sg_rules = new PolicyEntriesType();

            PolicyEntriesType.PolicyRuleType ingress_rule = 
                              new PolicyEntriesType.PolicyRuleType(
                                      null,
                                      UUID.randomUUID().toString(),
                                      ">",
                                      "any",
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, VNC_ROOT_DOMAIN + ":" + VNC_VCENTER_PROJECT + ":" + "default", null)}),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //src_ports
                                       null, //application
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, "local", null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //dest_ports
                                       null, // action_list
                                       "IPv4"); // ethertype
            sg_rules.addPolicyRule(ingress_rule);

            PolicyEntriesType.PolicyRuleType egress_rule  = 
                              new PolicyEntriesType.PolicyRuleType(
                                      null,
                                      UUID.randomUUID().toString(),
                                      ">",
                                      "any",
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(null, null, "local", null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //src_ports
                                       null, //application
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.AddressType[] {new PolicyEntriesType.PolicyRuleType.AddressType(new SubnetType("0.0.0.0", 0), null, null, null) }),
                                       Arrays.asList(new PolicyEntriesType.PolicyRuleType.PortType[] {new PolicyEntriesType.PolicyRuleType.PortType(0,65535)}), //dest_ports
                                       null, // action_list
                                       "IPv4"); // ethertype);
            sg_rules.addPolicyRule(egress_rule);

            vCenterDefSecGrp.setEntries(sg_rules);

            try {
                if (!apiConnector.create(vCenterDefSecGrp)) {
                    s_logger.error("Unable to create def sec grp: " + vCenterDefSecGrp.getName());
                }
            } catch (Exception e) {
                s_logger.error("Exception : " + e);
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error(stackTrace);
                return false;
            }
        } else {
            s_logger.info(" vCenter default sec-group present, continue ");
        }


        return true;
    }


    private void DeleteVirtualMachineInternal(
            VirtualMachineInterface vmInterface) throws IOException {

        String vmInterfaceUuid = vmInterface.getUuid();
        s_logger.debug("Delete Virtual Machine given VMI (uuid = " + vmInterfaceUuid + ")");

        // Clear security-group associations if it exists on VMInterface
        List<ObjectReference<ApiPropertyBase>> secGroupRefs = 
                vmInterface.getSecurityGroup();
        if ((secGroupRefs != null) && !secGroupRefs.isEmpty()) {
            s_logger.info("SecurityGroup association exists for VMInterface:" + vmInterface.getUuid());
            SecurityGroup secGroup = (SecurityGroup)
                apiConnector.findById(SecurityGroup.class, 
                                      secGroupRefs.get(0).getUuid());
            VirtualMachineInterface vmi = new VirtualMachineInterface();
            vmi.setParent(vmInterface.getParent());
            vmi.setName(vmInterface.getName());
            vmi.setUuid(vmInterface.getUuid());
            vmi.addSecurityGroup(secGroup);
            vmi.clearSecurityGroup();
            apiConnector.update(vmi);
            vmInterface.clearSecurityGroup();
            s_logger.info("Removed SecurityGroup association for VMInterface:" + vmInterface.getUuid());
        }

        // Clear flloating-ip associations if it exists on VMInterface
        List<ObjectReference<ApiPropertyBase>> floatingIpRefs = 
                vmInterface.getFloatingIpBackRefs();
        if ((floatingIpRefs != null) && !floatingIpRefs.isEmpty()) {
            s_logger.info("floatingIp association exists for VMInterface:" + vmInterface.getUuid());
            // there can be one floating-ip per VMI.
            FloatingIp floatingIp = (FloatingIp)
                apiConnector.findById(FloatingIp.class, 
                                      floatingIpRefs.get(0).getUuid());
            // clear VMInterface back reference.
            FloatingIp fip = new FloatingIp();
            fip.setParent(floatingIp.getParent());
            fip.setName(floatingIp.getName());
            fip.setUuid(floatingIp.getUuid());
            fip.setVirtualMachineInterface(vmInterface);
            fip.clearVirtualMachineInterface();
            apiConnector.update(fip);
            floatingIp.clearVirtualMachineInterface();
            s_logger.info("Removed floatingIp association for VMInterface:" + vmInterface.getUuid());
        }
           
        // delete instancIp
        List<ObjectReference<ApiPropertyBase>> instanceIpRefs = 
                vmInterface.getInstanceIpBackRefs();
        for (ObjectReference<ApiPropertyBase> instanceIpRef : 
            Utils.safe(instanceIpRefs)) {
            s_logger.info("Delete instance IP: " + 
                    instanceIpRef.getReferredName());
            apiConnector.delete(InstanceIp.class, 
                    instanceIpRef.getUuid());
        }

        // There should only be one virtual machine hanging off the virtual
        // machine interface
        List<ObjectReference<ApiPropertyBase>> vmRefs = vmInterface.getVirtualMachine();
        if (vmRefs == null || vmRefs.size() == 0) {
            s_logger.error("Virtual Machine Interface : " + vmInterface.getDisplayName() +
                    " NO associated virtual machine ");
            // delete VMInterface
            s_logger.info("Delete virtual machine interface: " +
                          vmInterface.getName());
            apiConnector.delete(vmInterface);
            return;
        }

        if (vmRefs.size() > 1) {
            s_logger.error("Virtual Machine Interface : " + vmInterface.getDisplayName() +
                           "is associated with" + "(" + vmRefs.size() + ")" + " virtual machines ");
        }

        ObjectReference<ApiPropertyBase> vmRef = vmRefs.get(0);
        VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                VirtualMachine.class, vmRef.getUuid());
        if (vm == null) {
            s_logger.warn("Virtual machine with uuid: " + vmRef.getUuid()
                          + " doesn't exist on api-server. Nothing to delete");
            return;
        }

        // If this is the only interface on this VM,
        // delete Virtual Machine as well after deleting last VMI
        boolean deleteVm = false;
        if (vm.getVirtualMachineInterfaceBackRefs().size() == 1) {
            deleteVm = true;
        }
        
        // delete VMInterface
        s_logger.info("Delete virtual machine interface: " + 
                vmInterface.getName());
        apiConnector.delete(vmInterface);

        // Send Unplug notification to vrouter
        String vrouterIpAddress = vm.getDisplayName();
        if (vrouterIpAddress != null) {
            ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
            if (vrouterApi == null) {
                vrouterApi = new ContrailVRouterApi(
                        InetAddress.getByName(vrouterIpAddress), 
                        vrouterApiPort, false, 1000);
                vrouterApiMap.put(vrouterIpAddress, vrouterApi);
            }
            vrouterApi.DeletePort(UUID.fromString(vmInterfaceUuid));
        } else {
            s_logger.warn("Virtual machine interace: " + vmInterfaceUuid + 
                    " DeletePort notification NOT sent");
        }

        // delete VirtualMachine or may-be-not 
        if (deleteVm == true) {
            apiConnector.delete(VirtualMachine.class, vm.getUuid());
            s_logger.info("Delete Virtual Machine (uuid = " + vm.getUuid() + ") Done.");
        } else {
            s_logger.info("Virtual Machine (uuid = " + vm.getUuid() + ") not deleted"
                          + " yet as more interfaces to be deleted.");
        }
    }

    public void DeleteVirtualMachine(VncVirtualMachineInfo vmInfo) 
            throws IOException {
        DeleteVirtualMachineInternal(vmInfo.getVmInterfaceInfo());
    }
    
    public void DeleteVirtualMachine(String vmUuid, String vnUuid, String vrouterIpAddress) throws IOException {

        s_logger.info("Delete Virtual Machine (vmUuid=" + vmUuid
                       + ", vnUuid=" + vnUuid + ")");

        VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                VirtualMachine.class, vmUuid);
        
        if (vm == null) {
            s_logger.warn("Virtual Machine (uuid = " + vmUuid + ") doesn't exist on VNC");
            return;
        }

        // Extract VRouter IP address from display name
        //String vrouterIpAddress = vm.getDisplayName();

        // Delete InstanceIp, VMInterface & VM
        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs =
                vm.getVirtualMachineInterfaceBackRefs();
        if ((vmInterfaceRefs == null) || (vmInterfaceRefs.size() == 0)) {
            s_logger.warn("Virtual Machine has NO interface");
            apiConnector.delete(VirtualMachine.class, vmUuid);
            s_logger.info("Delete Virtual Machine " + vm.getName() + "(uuid=" + vmUuid + ") Done.");
            return;
        }

        s_logger.info("Virtual Machine has " + vmInterfaceRefs.size() 
                      + " interfaces");
        boolean deleteVm = true;
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
             Utils.safe(vmInterfaceRefs)) {
            String vmInterfaceUuid = vmInterfaceRef.getUuid();
            VirtualMachineInterface vmInterface = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class, 
                            vmInterfaceUuid);
            List<ObjectReference<ApiPropertyBase>> vnRefs =
                                            vmInterface.getVirtualNetwork();
            if (!(vnRefs.get(0).getUuid().equals(vnUuid))) {
                continue;
            }

            // Found vmInterface matching vnUuuid
            s_logger.info("Found VMInterface matching" + " vnUuid = " + vnUuid);

            // If there are more than 1 interface on this VM,
            // don't delete Virtual Machine after deleting VMI
            if (vmInterfaceRefs.size() > 1) {
              deleteVm = false;
            }

            // Clear security-group associations if it exists on VMInterface
            List<ObjectReference<ApiPropertyBase>> secGroupRefs = 
                    vmInterface.getSecurityGroup();
            if ((secGroupRefs != null) && !secGroupRefs.isEmpty()) {
                s_logger.info("SecurityGroup association exists for VMInterface:" + vmInterface.getUuid());
                SecurityGroup secGroup = (SecurityGroup)
                    apiConnector.findById(SecurityGroup.class, 
                                          secGroupRefs.get(0).getUuid());
                VirtualMachineInterface vmi = new VirtualMachineInterface();
                vmi.setParent(vmInterface.getParent());
                vmi.setName(vmInterface.getName());
                vmi.setUuid(vmInterface.getUuid());
                vmi.addSecurityGroup(secGroup);
                vmi.clearSecurityGroup();
                apiConnector.update(vmi);
                vmInterface.clearSecurityGroup();
                s_logger.info("Removed SecurityGroup association for VMInterface:" + vmInterface.getUuid());
            }

            // Clear flloating-ip associations if it exists on VMInterface
            List<ObjectReference<ApiPropertyBase>> floatingIpRefs = 
                    vmInterface.getFloatingIpBackRefs();
            if ((floatingIpRefs != null) && !floatingIpRefs.isEmpty()) {
                s_logger.info("floatingIp association exists for VMInterface:" + vmInterface.getUuid());
                // there can be one floating-ip per VMI.
                FloatingIp floatingIp = (FloatingIp)
                    apiConnector.findById(FloatingIp.class, 
                                          floatingIpRefs.get(0).getUuid());
                // clear VMInterface back reference.
                FloatingIp fip = new FloatingIp();
                fip.setParent(floatingIp.getParent());
                fip.setName(floatingIp.getName());
                fip.setUuid(floatingIp.getUuid());
                fip.setVirtualMachineInterface(vmInterface);
                fip.clearVirtualMachineInterface();
                apiConnector.update(fip);
                floatingIp.clearVirtualMachineInterface();
                s_logger.info("Removed floatingIp association for VMInterface:" + vmInterface.getUuid());
            }
           
            // delete instancIp
            List<ObjectReference<ApiPropertyBase>> instanceIpRefs = 
                    vmInterface.getInstanceIpBackRefs();
            for (ObjectReference<ApiPropertyBase> instanceIpRef : 
                Utils.safe(instanceIpRefs)) {
                s_logger.info("Delete instance IP: " + 
                        instanceIpRef.getReferredName());
                apiConnector.delete(InstanceIp.class, 
                        instanceIpRef.getUuid());
            }

            // delete VMInterface
            s_logger.info("Delete virtual machine interface: " + 
                    vmInterface.getName());
            apiConnector.delete(VirtualMachineInterface.class,
                    vmInterfaceUuid);

            // Send Unplug notification to vrouter
            if (vrouterIpAddress == null) {
                s_logger.warn("Virtual machine interace: " + vmInterfaceUuid + 
                        " delete notification NOT sent");
                continue;
            }
            ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
            if (vrouterApi == null) {
                vrouterApi = new ContrailVRouterApi(
                        InetAddress.getByName(vrouterIpAddress), 
                        vrouterApiPort, false, 1000);
                vrouterApiMap.put(vrouterIpAddress, vrouterApi);
            }
            vrouterApi.DeletePort(UUID.fromString(vmInterfaceUuid));
        }

        // delete VirtualMachine or may-be-not 
        if (deleteVm == true) {
            apiConnector.delete(VirtualMachine.class, vmUuid);
            s_logger.info("Delete Virtual Machine " + vm.getName() + " (uuid=" + vmUuid + ") Done.");
        } else {
            s_logger.info("Virtual Machine :" + vm.getName() + " (uuid =" + vmUuid + ") not deleted"
                          + " yet as more interfaces to be deleted.");
        }
    }
    
    
    public void CreateVirtualMachine(String vnUuid, String vmUuid,
            String macAddress, String vmName, String vrouterIpAddress,
            String hostName, short isolatedVlanId, short primaryVlanId,
            boolean external_ipam, VmwareVirtualMachineInfo vmwareVmInfo) throws IOException {
        s_logger.info("Create Virtual Machine : " 
                       + "VM:" + vmName + " (uuid=" + vmUuid + ")"
                       + ", VN:" + vnUuid
                       + ", vrouterIp: " + vrouterIpAddress
                       + ", EsxiHost:" + hostName
                       + ", vlan:" + primaryVlanId + "/" + isolatedVlanId);
        
        // Virtual Network
        VirtualNetwork network = (VirtualNetwork) apiConnector.findById(
                VirtualNetwork.class, vnUuid);
        if (network == null) {
            s_logger.warn("Create Virtual Machine requested with invalid VN Uuid: " + vnUuid);
            return;
        }

        // Virtual Machine
        VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                VirtualMachine.class, vmUuid);
        if (vm == null) {
            // Create Virtual machine
            vm = new VirtualMachine();
            vm.setName(vmUuid);
            vm.setUuid(vmUuid);

            // Encode VRouter IP address in display name
            if (vrouterIpAddress != null) {
                vm.setDisplayName(vrouterIpAddress);
            }
            vm.setIdPerms(vCenterIdPerms);
            apiConnector.create(vm);
            apiConnector.read(vm);
        }

        // find VMI matching vmUuid & vnUuid
        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs =
                vm.getVirtualMachineInterfaceBackRefs();
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
            Utils.safe(vmInterfaceRefs)) {
            String vmInterfaceUuid = vmInterfaceRef.getUuid();
            VirtualMachineInterface vmInterface = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class, 
                            vmInterfaceUuid);
            List<ObjectReference<ApiPropertyBase>> vnRefs =
                                            vmInterface.getVirtualNetwork();
            for (ObjectReference<ApiPropertyBase> vnRef : vnRefs) {
                if (vnRef.getUuid().equals(vnUuid)) {
                    s_logger.debug("VMI exits with vnUuid =" + vnUuid 
                                 + " vmUuid = " + vmUuid + " no need to create new ");
                    return;
                }
            }
        }

        // create Virtual machine interface
        String vmInterfaceName = "vmi-" + network.getName() + "-" + vmName;
        String vmiUuid = UUID.randomUUID().toString();
        VirtualMachineInterface vmInterface = new VirtualMachineInterface();
        vmInterface.setDisplayName(vmInterfaceName);
        vmInterface.setUuid(vmiUuid);
        vmInterface.setParent(vCenterProject);
        vmInterface.setSecurityGroup(vCenterDefSecGrp);
        vmInterface.setName(vmiUuid);
        vmInterface.setVirtualNetwork(network);
        vmInterface.addVirtualMachine(vm);
        MacAddressesType macAddrType = new MacAddressesType();
        macAddrType.addMacAddress(macAddress);
        vmInterface.setMacAddresses(macAddrType);
        vmInterface.setIdPerms(vCenterIdPerms);
        apiConnector.create(vmInterface);
        vmwareVmInfo.setInterfaceUuid(vmiUuid);
        s_logger.debug("Created virtual machine interface:" + vmInterfaceName + 
                ", vmiUuid:" + vmiUuid);

        // Instance Ip
        String vmIpAddress = "0.0.0.0";
        if (external_ipam != true) {
            String instanceIpName = "ip-" + network.getName() + "-" + vmName;
            String instIpUuid = UUID.randomUUID().toString();
            InstanceIp instanceIp = new InstanceIp();
            instanceIp.setDisplayName(instanceIpName);
            instanceIp.setUuid(instIpUuid);
            instanceIp.setName(instIpUuid);
            instanceIp.setVirtualNetwork(network);
            instanceIp.setVirtualMachineInterface(vmInterface);
            instanceIp.setIdPerms(vCenterIdPerms);
            apiConnector.create(instanceIp);

            // Read back to get assigned IP address
            apiConnector.read(instanceIp);
            vmIpAddress = instanceIp.getAddress();
            s_logger.debug("Created instanceIP:" + instanceIp.getName() + ": " +
                            vmIpAddress);
        }

        // Plug notification to vrouter
        if (vrouterIpAddress == null) {
            s_logger.warn("Virtual machine: " + vmName + " esxi host: " + hostName
                + " addPort notification NOT sent as vRouterIp Address not known");
            return;
        }
        try {
            ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
            if (vrouterApi == null) {
                   vrouterApi = new ContrailVRouterApi(
                         InetAddress.getByName(vrouterIpAddress), 
                         vrouterApiPort, false, 1000);
                   vrouterApiMap.put(vrouterIpAddress, vrouterApi);
            }
            if (vmwareVmInfo.isPoweredOnState()) {
                boolean ret = vrouterApi.AddPort(UUID.fromString(vmiUuid),
                                         UUID.fromString(vmUuid), vmInterface.getName(),
                                         InetAddress.getByName(vmIpAddress),
                                         Utils.parseMacAddress(macAddress),
                                         UUID.fromString(vnUuid), isolatedVlanId, 
                                         primaryVlanId, vmName);
                if ( ret == true) {
                    s_logger.info("VRouterAPi Add Port success - interface name:"
                                  +  vmInterface.getDisplayName()
                                  + "(" + vmInterface.getName() + ")"
                                  + ", VM=" + vmName
                                  + ", VN=" + network.getName()
                                  + ", vmIpAddress=" + vmIpAddress
                                  + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
                } else {
                    // log failure but don't worry. Periodic KeepAlive task will
                    // attempt to connect to vRouter Agent and replay AddPorts.
                    s_logger.error("VRouterAPi Add Port failed - interface name: "
                                  +  vmInterface.getDisplayName()
                                  + "(" + vmInterface.getName() + ")"
                                  + ", VM=" + vmName
                                  + ", VN=" + network.getName()
                                  + ", vmIpAddress=" + vmIpAddress
                                  + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
                }
            } else {
                s_logger.info("VM (" + vmName + ") is PoweredOff. Skip AddPort now.");
            }
        }catch(Throwable e) {
            s_logger.error("Exception : " + e);
            e.printStackTrace();
        }
        s_logger.info("Create Virtual Machine :"
                       + " VM:" + vmName + " (uuid=" + vmUuid + ") Done");
    }

    public void CreateVMInterfaceInstanceIp(String vnUuid, String vmUuid,
            VmwareVirtualMachineInfo vmwareVmInfo) throws IOException {
        s_logger.info("Create VM instanceIp : "
                       + ", VM:" + vmUuid
                       + ", VN:" + vnUuid
                       + ", requested IP:" + vmwareVmInfo.getIpAddress());

        // Virtual Network
        VirtualNetwork network = (VirtualNetwork) apiConnector.findById(
                VirtualNetwork.class, vnUuid);
        if (network == null) {
            s_logger.warn("Create VM InstanceIp requested with invalid VN: " + vnUuid);
            return;
        }

        // Virtual Machine
        VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                VirtualMachine.class, vmUuid);
        if (vm == null) {
            s_logger.warn("Create VM InstanceIp requested with invalid VM: " + vmUuid
                          + "and valid VN=" + network.getName() + "(" + vnUuid + ")");
            return;
        }

        s_logger.info("Create VM instanceIp : "
                       + ", VM Name:" + vm.getName()
                       + ", VN Name:" + network.getName());

        // find VMI matching vmUuid & vnUuid
        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs =
                vm.getVirtualMachineInterfaceBackRefs();
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
            Utils.safe(vmInterfaceRefs)) {
            String vmInterfaceUuid = vmInterfaceRef.getUuid();
            VirtualMachineInterface vmInterface = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class,
                            vmInterfaceUuid);
            List<ObjectReference<ApiPropertyBase>> vnRefs =
                                            vmInterface.getVirtualNetwork();
            for (ObjectReference<ApiPropertyBase> vnRef : vnRefs) {
                if (vnRef.getUuid().equals(vnUuid)) {
                    s_logger.info("VMI exits with vnUuid =" + vnUuid
                                 + " vmUuid = " + vmUuid + " no need to create new VMI");

                    // check if instance-ip exists
                    List<ObjectReference<ApiPropertyBase>> instIpRefs =
                                            vmInterface.getInstanceIpBackRefs();
                    if ((instIpRefs != null) && !instIpRefs.isEmpty()) {
                        ObjectReference<ApiPropertyBase> ipRef = instIpRefs.get(0);
                        InstanceIp instIp = (InstanceIp) apiConnector.findById(
                                                  InstanceIp.class, ipRef.getUuid());

                        if (instIp.getAddress().equals(vmwareVmInfo.getIpAddress())) {
                            // same instanceIp.
                            s_logger.info("VM instanceIp (" + vmwareVmInfo.getIpAddress() +
                                           ") exists on VNC ..skip creation and return" );
                            s_logger.info("Create VM instanceIp : Done");
                            return;
                        }
                        // ip address on interface changed.
                        // delete old ip
                        s_logger.info("Deleting previus instance IP:" + instIp.getName() + ": " +
                                        instIp.getAddress());
                        apiConnector.delete(instIp);
                    }

                    // Add new ip address to interface
                    if (vmwareVmInfo.getIpAddress() != null) {
                        String instanceIpName = "ip-" + network.getName() + "-" + vm.getName();
                        String instIpUuid = UUID.randomUUID().toString();
                        InstanceIp instanceIp = new InstanceIp();
                        instanceIp.setDisplayName(instanceIpName);
                        instanceIp.setUuid(instIpUuid);
                        instanceIp.setName(instIpUuid);
                        instanceIp.setVirtualNetwork(network);
                        instanceIp.setVirtualMachineInterface(vmInterface);
                        instanceIp.setIdPerms(vCenterIdPerms);
                        instanceIp.setAddress(vmwareVmInfo.getIpAddress());
                        apiConnector.create(instanceIp);
                        s_logger.info("Created instanceIP:" + instanceIp.getName() + ": " +
                                        instanceIp.getAddress());
                    }
                }
            }
        }
        s_logger.info("Create VM instanceIp : Done");
    }

    public void VifPlug(String vnUuid, String vmUuid,
            String macAddress, String vmName, String vrouterIpAddress,
            String hostName, short isolatedVlanId, short primaryVlanId,
            VmwareVirtualMachineInfo vmwareVmInfo) throws IOException {
        s_logger.info("VifPlug : "
                      + " VN:" + vnUuid
                      + ", VM:" + vmName + " (" + vmUuid + ")"
                      + ", vrouterIp:" + vrouterIpAddress
                      + ", EsxiHost:" + hostName
                      + ", vlan:" + primaryVlanId + "/" + isolatedVlanId);

        // Virtual network
        VirtualNetwork network = (VirtualNetwork) apiConnector.findById(
                VirtualNetwork.class, vnUuid);

        // Virtual machine
        VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                VirtualMachine.class, vmUuid);

        // find Virtual Machine Interfce matching vmUuid & vnUuid
        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs =
                vm.getVirtualMachineInterfaceBackRefs();
        VirtualMachineInterface vmInterface = null;
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
                Utils.safe(vmInterfaceRefs)) {
            VirtualMachineInterface vmiTmp = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class,
                            vmInterfaceRef.getUuid());

            if (vmiTmp == null) {
                s_logger.warn("Virtual Machine (" + vmName
                              + ") has VMI ref (uuid=" + vmInterfaceRef.getUuid()
                              + ") but no VMI exists with the given Uuid");
                continue;
            }
            List<ObjectReference<ApiPropertyBase>> vnRefs =
                                            vmiTmp.getVirtualNetwork();
            for (ObjectReference<ApiPropertyBase> vnRef : vnRefs) {
                if (vnRef.getUuid().equals(vnUuid)) {
                    vmInterface = vmiTmp;
                }
            }
        }
        if (vmInterface == null) {
            s_logger.warn("Virtual machine: " + vmName
                          + " has no VMI matching network Uuid="
                          + vnUuid);
            return;
        }

        // Instance Ip
        // Read back to get assigned IP address
        List<ObjectReference<ApiPropertyBase>> instanceIpBackRefs =
                vmInterface.getInstanceIpBackRefs();
        InstanceIp instanceIp = null;
        for (ObjectReference<ApiPropertyBase> instanceIpRef :
                Utils.safe(instanceIpBackRefs)) {
            instanceIp = (InstanceIp)
                    apiConnector.findById(InstanceIp.class,
                            instanceIpRef.getUuid());
        }

        String vmIpAddress = "0.0.0.0";
        if (instanceIp != null) {
            vmIpAddress = instanceIp.getAddress();
        }

        // Plug notification to vrouter
        if (vrouterIpAddress == null) {
            s_logger.warn("Virtual machine: " + vmName + " EsxiHost: " + hostName
                + " AddPort notification NOT sent since vrouter-ip address missing");
            s_logger.info("VifPlug : Done");
            return;
        }
        vmwareVmInfo.setInterfaceUuid(vmInterface.getUuid());

        try {
            ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
            if (vrouterApi == null) {
                   vrouterApi = new ContrailVRouterApi(
                         InetAddress.getByName(vrouterIpAddress),
                         vrouterApiPort, false, 1000);
                   vrouterApiMap.put(vrouterIpAddress, vrouterApi);
            }
            boolean ret = vrouterApi.AddPort(UUID.fromString(vmInterface.getUuid()),
                               UUID.fromString(vmUuid), vmInterface.getName(),
                               InetAddress.getByName(vmIpAddress),
                               Utils.parseMacAddress(macAddress),
                               UUID.fromString(vnUuid), isolatedVlanId,
                               primaryVlanId, vmName);
            if ( ret == true) {
                s_logger.info("VRouterAPi Add Port success - interface name: "
                              +  vmInterface.getDisplayName()
                              + "(" + vmInterface.getName() + "),"
                              + ", VM=" + vmName
                              + ", VN=" + network.getName()
                              + ", vmIpAddress=" + vmIpAddress
                              + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
            } else {
                // log failure but don't worry. Periodic KeepAlive task will
                // attempt to connect to vRouter Agent and replay AddPorts.
                s_logger.error("VRouterAPi Add Port failed - interface name: "
                              +  vmInterface.getDisplayName()
                              + "(" + vmInterface.getName() + ")"
                              + ", VM=" + vmName
                              + ", VN=" + network.getName()
                              + ", vmIpAddress=" + vmIpAddress
                              + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
            }
        }catch(Throwable e) {
            s_logger.error("Exception : " + e);
            e.printStackTrace();
        }
        s_logger.info("VifPlug for"
                      + " VM:" + vmName + " (" + vmUuid + ") Done");
    }

    void VifUnplug(String vmInterfaceUuid, String vrouterIpAddress)
                    throws IOException {

        s_logger.info("VifUnplug  VMI:" + vmInterfaceUuid);

        if (vmInterfaceUuid == null) {
            s_logger.warn("Virtual machine interface UUID is null" );
            s_logger.info("DeletePort  VMI:null Skipped");
            return;
        }
        // Unplug notification to vrouter
        if (vrouterIpAddress == null) {
            s_logger.warn("Virtual machine interface: " + vmInterfaceUuid +
                    " deletePORT  notification NOT sent as vRouter Ip is NULL");
            s_logger.info("DeletePort  VMI: " + vmInterfaceUuid + " Skipped");
            return;
        }
        ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
        if (vrouterApi == null) {
            vrouterApi = new ContrailVRouterApi(
                    InetAddress.getByName(vrouterIpAddress),
                    vrouterApiPort, false, 1000);
            vrouterApiMap.put(vrouterIpAddress, vrouterApi);
        }
        boolean ret = vrouterApi.DeletePort(UUID.fromString(vmInterfaceUuid));
        if ( ret == true) {
            s_logger.info("VRouterAPi Delete Port success - VMI: "
                          + vmInterfaceUuid + ")");
        } else {
            // log failure but don't worry. Periodic KeepAlive task will
            // attempt to connect to vRouter Agent and ports that are not
            // replayed by client(plugin) will be deleted by vRouter Agent.
            s_logger.info("VRouterAPi Delete Port failure - VMI: "
                          + vmInterfaceUuid + ")");
        }
        s_logger.info("VifUnplug  VMI:" + vmInterfaceUuid + " Done");
    }

    public void CreateVirtualNetwork(String vnUuid, String vnName,
            String subnetAddr, String subnetMask, String gatewayAddr, 
            short isolatedVlanId, short primaryVlanId,
            boolean ipPoolEnabled, String range, boolean externalIpam,
            SortedMap<String, VmwareVirtualMachineInfo> vmMapInfos) throws
            IOException {
        s_logger.info("Create Virtual Network: " 
                        + vnName + " (" + vnUuid + ")"
                        + ", Subnet/Mask/GW: " 
                        + subnetAddr + "/" + subnetMask + "/" + gatewayAddr
                        + ", externalIpam:" + externalIpam);
        VirtualNetwork vn = new VirtualNetwork();
        vn.setName(vnName);
        vn.setDisplayName(vnName);
        vn.setUuid(vnUuid);
        vn.setIdPerms(vCenterIdPerms);
        vn.setParent(vCenterProject);
        vn.setExternalIpam(externalIpam);
        SubnetUtils subnetUtils = new SubnetUtils(subnetAddr, subnetMask);  
        String cidr = subnetUtils.getInfo().getCidrSignature();
        String[] addr_pair = cidr.split("\\/");

        List<VnSubnetsType.IpamSubnetType.AllocationPoolType> allocation_pools = null;
        if (ipPoolEnabled == true && !range.isEmpty()) {
            String[] pools = range.split("\\#");
            if (pools.length == 2) {
                allocation_pools = new ArrayList<VnSubnetsType.IpamSubnetType.AllocationPoolType>();
                String start = (pools[0]).replace(" ","");
                String num   = (pools[1]).replace(" ","");
                String[] bytes = start.split("\\.");
                String end   = bytes[0] + "." + bytes[1] + "." + bytes[2] + "."
                               + Integer.toString(Integer.parseInt(bytes[3]) +  Integer.parseInt(num) - 1);
                s_logger.info("Subnet IP Range :  Start:"  + start + " End:" + end);
                VnSubnetsType.IpamSubnetType.AllocationPoolType pool1 = new 
                       VnSubnetsType.IpamSubnetType.AllocationPoolType(start, end);
                allocation_pools.add(pool1);
            }

        }

        VnSubnetsType subnet = new VnSubnetsType();
        subnet.addIpamSubnets(new VnSubnetsType.IpamSubnetType(
                                   new SubnetType(addr_pair[0],
                                       Integer.parseInt(addr_pair[1])),
                                       gatewayAddr,
                                       null,                          // dns_server_address
                                       UUID.randomUUID().toString(),  // subnet_uuid
                                       true,                          // enable_dhcp
                                       null,                          // dns_nameservers
                                       allocation_pools,
                                       true,                          // addr_from_start
                                       null,                          // dhcp_options_list
                                       null,                          // host_routes
                                       vn.getName() + "-subnet"));

        vn.setNetworkIpam(vCenterIpam, subnet);
        apiConnector.create(vn); 
        if (vmMapInfos == null) {
            s_logger.info("No Virtual Machines present on the network.");
            s_logger.info("Create Virtual Network: Done");
            return;
        }

        s_logger.info("Total " + vmMapInfos.size() + "VMs present on the network.");
        s_logger.info("Create VMs on VNC and perform AddPort as requried");
        for (Map.Entry<String, VmwareVirtualMachineInfo> vmMapInfo :
            vmMapInfos.entrySet()) {
            String vmUuid = vmMapInfo.getKey();
            VmwareVirtualMachineInfo vmInfo = vmMapInfo.getValue();
            String macAddress = vmInfo.getMacAddress();
            String vmName = vmInfo.getName();
            String vrouterIpAddr = vmInfo.getVrouterIpAddress();
            String hostName = vmInfo.getHostName();
            CreateVirtualMachine(vnUuid, vmUuid, macAddress, vmName,
                    vrouterIpAddr, hostName, isolatedVlanId, primaryVlanId,
                    externalIpam, vmInfo);
            if ((vmInfo.isPoweredOnState() == true)
                && (externalIpam == true)
                && (vmInfo.getIpAddress() != null) ) {
                CreateVMInterfaceInstanceIp(vnUuid, vmUuid, vmInfo);
            }
        }
        s_logger.info("Create Virtual Network: Done");
    }
    

    public void DeleteVirtualNetwork(String uuid) 
            throws IOException {
        if (uuid == null) {
            s_logger.info("Delete virtual network: null");
            s_logger.warn("Virtual network delete request with null uuid... Return");
            return;
        }
        VirtualNetwork network = (VirtualNetwork) apiConnector.findById(
                VirtualNetwork.class, uuid);
        if (network == null) {
            s_logger.info("Delete virtual network: " + uuid);
            s_logger.warn("Virtual network with uuid =" + uuid + "doesn't exist");
            return;
        }
        s_logger.info("Delete virtual network: " + network.getName() +
                     " (" + uuid + ")");

        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs = 
                network.getVirtualMachineInterfaceBackRefs();
        if (vmInterfaceRefs == null || vmInterfaceRefs.size() == 0) {
            s_logger.debug("Virtual network: " + network + 
                    " NO associated virtual machine interfaces");
            apiConnector.delete(VirtualNetwork.class, network.getUuid());     
            s_logger.info("Delete virtual network: " + network.getName() + " Done");
            return;
        }
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef : 
                Utils.safe(vmInterfaceRefs)) {
            VirtualMachineInterface vmInterface = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class,
                            vmInterfaceRef.getUuid());
            if (vmInterface == null) {
                continue;
            }
            DeleteVirtualMachineInternal(vmInterface);
        }
        apiConnector.delete(VirtualNetwork.class, network.getUuid());     
        s_logger.info("Delete virtual network: " + network.getName() + " Done");
    }
    
    protected static boolean doIgnoreVirtualNetwork(String name) {
        // Ignore default, fabric, and link-local networks
        if (name.equals("__link_local__") || 
                name.equals("default-virtual-network") || 
                name.equals("ip-fabric")) {
            return true;
        }
        return false;
    }
    
    @SuppressWarnings("unchecked")
    public SortedMap<String, VncVirtualNetworkInfo> populateVirtualNetworkInfo() 
        throws Exception {
        // Extract list of virtual networks
        List<VirtualNetwork> networks = null;
        try {
        networks = (List<VirtualNetwork>) 
                apiConnector.list(VirtualNetwork.class, null);
        } catch (Exception ex) {
            s_logger.error("Exception in api.list(VirtualNetorks): " + ex);
            ex.printStackTrace();
        }
        if (networks == null || networks.size() == 0) {
            s_logger.debug("NO virtual networks FOUND");
            return null;
        }
        SortedMap<String, VncVirtualNetworkInfo> vnInfos =
                new TreeMap<String, VncVirtualNetworkInfo>();
        for (VirtualNetwork network : networks) {
            // Read in the virtual network
            apiConnector.read(network);
            String vnName = network.getName();
            String vnUuid = network.getUuid();
            // Ignore network ?
            if (doIgnoreVirtualNetwork(vnName)) {
                continue;
            }
            // Ignore Vnc VNs where creator isn't "vcenter-plugin"
            if ((network.getIdPerms().getCreator() == null)  ||
                !(network.getIdPerms().getCreator().equals(VNC_VCENTER_PLUGIN))) {
                continue;
            }

            // Extract virtual machine interfaces
            List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs = 
                    network.getVirtualMachineInterfaceBackRefs();
            if (vmInterfaceRefs == null || vmInterfaceRefs.size() == 0) {
                s_logger.debug("Virtual network: " + network + 
                        " NO associated virtual machine interfaces");
            }
            SortedMap<String, VncVirtualMachineInfo> vmInfos = 
                    new TreeMap<String, VncVirtualMachineInfo>();
            for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
                Utils.safe(vmInterfaceRefs)) {

                if (vmInterfaceRef == null) {
                    continue;
                }

                VirtualMachineInterface vmInterface =
                        (VirtualMachineInterface) apiConnector.findById(
                                VirtualMachineInterface.class,
                                vmInterfaceRef.getUuid());
                if (vmInterface == null) {
                    continue;
                }
                // Ignore Vnc VMInterfaces where "creator" isn't "vcenter-plugin"
                if (vmInterface.getIdPerms().getCreator() == null) {
                    continue;
                }
                if (!vmInterface.getIdPerms().getCreator().equals(VNC_VCENTER_PLUGIN)) {
                    continue;
                }
                //String vmUuid = vmInterface.getParentUuid();
                List<ObjectReference<ApiPropertyBase>> vmRefs = vmInterface.getVirtualMachine();
                if (vmRefs == null || vmRefs.size() == 0) {
                    s_logger.error("Virtual Machine Interface : " + vmInterface.getDisplayName() +
                            " NO associated virtual machine ");
                }
                if (vmRefs.size() > 1) {
                    s_logger.error("Virtual Machine Interface : " + vmInterface.getDisplayName() +
                                   "(" + vmRefs.size() + ")" + " associated virtual machines ");
                }

                ObjectReference<ApiPropertyBase> vmRef = vmRefs.get(0);
                VirtualMachine vm = (VirtualMachine) apiConnector.findById(
                        VirtualMachine.class, vmRef.getUuid());
                apiConnector.read(vm);
                // Ignore Vnc VMs where creator isn't "vcenter-plugin"
                if (!vm.getIdPerms().getCreator().equals(VNC_VCENTER_PLUGIN)) {
                    continue;
                }

                VncVirtualMachineInfo vmInfo = new VncVirtualMachineInfo(
                        vm, vmInterface);
                vmInfos.put(vm.getUuid(), vmInfo);
            }
            VncVirtualNetworkInfo vnInfo = 
                    new VncVirtualNetworkInfo(vnName, vmInfos);
            vnInfos.put(vnUuid, vnInfo);
        }
        if (vnInfos.size() == 0) {
            s_logger.debug("NO virtual networks found");
        }
        return vnInfos;
    }

    // KeepAlive with all active vRouter Agent Connections.
    public void vrouterAgentPeriodicConnectionCheck(Map<String, Boolean> vRouterActiveMap) {
        for (Map.Entry<String, Boolean> entry: vRouterActiveMap.entrySet()) {
            if (entry.getValue() == Boolean.FALSE) {
                // host is in maintenance mode
                continue;
            }

            String vrouterIpAddress = entry.getKey();
            ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
            if (vrouterApi == null) {
                try {
                    vrouterApi = new ContrailVRouterApi(
                          InetAddress.getByName(vrouterIpAddress), 
                          vrouterApiPort, false, 1000);
                } catch (UnknownHostException e) { 
                }
                if (vrouterApi == null) {
                    continue;
                }
                vrouterApiMap.put(vrouterIpAddress, vrouterApi);
            }
            // run Keep Alive with vRouter Agent.
            vrouterApi.PeriodicConnectionCheck();
        }
    }

    public void createOrUpdateVmApiObjects(VmwareVirtualMachineInfo vmInfo, VirtualMachine vm)
            throws IOException {
        createOrUpdateApiVm(vmInfo, vm);

         // loop through all the networks in which
         // this VM participates and create VMIs and IP Instances
         for (Map.Entry<String, VmwareVirtualMachineInterfaceInfo> entry: 
                 vmInfo.getVmiInfo().entrySet()) {
             VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
             VmwareVirtualNetworkInfo vnInfo = vmiInfo.getVnInfo();
             //createOrUpdateApiVn(vnInfo);
             createOrUpdateApiVmi(vmiInfo);
             if (vnInfo.getExternalIpam() == false) {
                 createOrUpdateApiInstanceIp(vmiInfo);
             }
         }
    }
    
    public void createOrUpdateVmApiObjects(VmwareVirtualMachineInfo vmInfo)
            throws IOException {
        createOrUpdateVmApiObjects(vmInfo, null);
    }

    public void deleteVmApiObjects(VmwareVirtualMachineInfo vmInfo)
            throws IOException {    
         // loop through all the networks in which
         // this VM participates and delete VMIs and IP Instances
         for (Map.Entry<String, VmwareVirtualMachineInterfaceInfo> entry: 
                 vmInfo.getVmiInfo().entrySet()) {
             VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
             VmwareVirtualNetworkInfo vnInfo = vmiInfo.getVnInfo();
             
             if (vnInfo.getExternalIpam() == false) {
                 deleteApiInstanceIp(vmiInfo);
             }
             deleteApiVmi(vmiInfo);
         }
         
         deleteApiVm(vmInfo);
    }

    public void createOrUpdateVnApiObjects(VmwareVirtualNetworkInfo vnInfo)
            throws IOException {
        createOrUpdateApiVn(vnInfo, null);
    }

    public void createOrUpdateVnApiObjects(VmwareVirtualNetworkInfo vnInfo,
            VirtualNetwork vn)
            throws IOException {
        createOrUpdateApiVn(vnInfo, vn);
    }
    
    /*
    public void createVirtualNetworks(
            Map<String, VmwareVirtualNetworkInfo> vnInfoMap)
            throws IOException {
        Iterator<Entry<String, VmwareVirtualNetworkInfo>> iter = 
                vnInfoMap.entrySet().iterator();
        
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualNetworkInfo> entry = iter.next();
            createOrUpdateApiVn(entry.getValue());
        }
    }

    public void createVirtualMachines(
            Map<String, VmwareVirtualMachineInfo> vmInfoMap)
            throws IOException {
        Iterator<Entry<String, VmwareVirtualMachineInfo>> iter = 
                vmInfoMap.entrySet().iterator();
        
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualMachineInfo> entry = iter.next();
            createOrUpdateVmApiObjects(entry.getValue());
        }
    }*/

    public void deleteVnApiObjects(VmwareVirtualNetworkInfo vnInfo)
            throws IOException {
        deleteApiVn(vnInfo);
    }

    public void createOrUpdateApiVn(VmwareVirtualNetworkInfo vnInfo, VirtualNetwork vn)
            throws IOException {
        if (vnInfo == null) {
            s_logger.error("Incomplete information for creating API VN");
            throw new IllegalArgumentException();
        }
        String vnUuid = vnInfo.getUuid();
        String descr = "Virtual Network <"
                + vnInfo.getName()
                + ", UUID " + vnUuid
                + ", vlan " + vnInfo.getPrimaryVlanId() + "/"
                + vnInfo.getIsolatedVlanId() + ">";

        boolean create = false;
        if (vn == null) {
            
             vn = (VirtualNetwork) apiConnector.findById(
                     VirtualNetwork.class, vnUuid);
    
             if (vn == null) {
                 create = true;
                 vn = new VirtualNetwork();
             }
        }
        
        vn.setName(vnInfo.getName());
        vn.setDisplayName(vnInfo.getName());
        vn.setUuid(vnInfo.getUuid());
        vn.setIdPerms(vCenterIdPerms);
        vn.setParent(vCenterProject);
        vn.setExternalIpam(vnInfo.getExternalIpam());
        
         //TODO
         /*
         SubnetUtils subnetUtils = new SubnetUtils(vnInfo.getSubnetAddress(), vnInfo.getSubnetMask());
         String cidr = subnetUtils.getInfo().getCidrSignature();
         String[] addr_pair = cidr.split("\\/");
        
         List<VnSubnetsType.IpamSubnetType.AllocationPoolType> allocation_pools = null;
         if (ipPoolEnabled == true && !range.isEmpty()) {
             String[] pools = range.split("\\#");
             if (pools.length == 2) {
                 allocation_pools = new ArrayList<VnSubnetsType.IpamSubnetType.AllocationPoolType>();
                 String start = (pools[0]).replace(" ","");
                 String num   = (pools[1]).replace(" ","");
                 String[] bytes = start.split("\\.");
                 String end   = bytes[0] + "." + bytes[1] + "." + bytes[2] + "."
                                + Integer.toString(Integer.parseInt(bytes[3]) +  Integer.parseInt(num) - 1);
                 s_logger.info("Subnet IP Range :  Start:"  + start + " End:" + end);
                 VnSubnetsType.IpamSubnetType.AllocationPoolType pool1 = new
                        VnSubnetsType.IpamSubnetType.AllocationPoolType(start, end);
                 allocation_pools.add(pool1);
             }
         }
        
         VnSubnetsType subnet = new VnSubnetsType();
         subnet.addIpamSubnets(new VnSubnetsType.IpamSubnetType(
                                    new SubnetType(addr_pair[0],
                                        Integer.parseInt(addr_pair[1])),
                                        gatewayAddr,
                                        null,                          // dns_server_address
                                        UUID.randomUUID().toString(),  // subnet_uuid
                                        true,                          // enable_dhcp
                                        null,                          // dns_nameservers
                                        allocation_pools,
                                        true,                          // addr_from_start
                                        null,                          // dhcp_options_list
                                        null,                          // host_routes
                                        vn.getName() + "-subnet"));
        
         vn.setNetworkIpam(vCenterIpam, subnet);
         */
         if (create) {
             s_logger.info("Create " + descr);
             apiConnector.create(vn);
         } else {
             s_logger.info("Update " + descr);
             apiConnector.update(vn);
         }
         apiConnector.read(vn);
         
         vnInfo.apiVn = vn;
    }

    public void deleteApiVn(VmwareVirtualNetworkInfo vnInfo)
            throws IOException {
        if (vnInfo == null || vnInfo.apiVn == null) {
            s_logger.error("Cannot delete API VN: null arguments");
            throw new IllegalArgumentException("Null arguments");
        }

        apiConnector.delete(vnInfo.apiVn);
        vnInfo.apiVn = null;
        s_logger.info("Deleted " + vnInfo.apiVn);
    }

    public void createOrUpdateApiVm(VmwareVirtualMachineInfo vmInfo, VirtualMachine vm)
            throws IOException {
        if (vmInfo == null) {
            s_logger.error("Incomplete information for creating VM in the Api Server");
            throw new IllegalArgumentException("vmInfo is null");
        }

        String vmUuid = vmInfo.getUuid();
        String descr = "VM <" + vmInfo.getName()
                + ", EsxiHost:" + vmInfo.getHostName()
                + ", uuid " + vmUuid
                + ", vrouterIp: " + vmInfo.getVrouterIpAddress() + ">";

         // Virtual Machine
         boolean create = false;
         if (vm == null) {
             vm = (VirtualMachine) apiConnector.findById(
                     VirtualMachine.class, vmUuid);
    
             if (vm == null) {
                 create = true;
                 vm = new VirtualMachine();
             }
         }
         vm.setName(vmInfo.getName());
         vm.setUuid(vmUuid);

         // Encode VRouter IP address in display name
         if (vmInfo.getVrouterIpAddress() != null) {
             vm.setDisplayName(vmInfo.getVrouterIpAddress());
         }
         vm.setIdPerms(vCenterIdPerms);
         if (create) {
             s_logger.info("Create " + descr);
             apiConnector.create(vm);
         } else {
             s_logger.info("Update " + descr);
             apiConnector.update(vm);
         }

         apiConnector.read(vm);
         
         vmInfo.apiVm = vm;
    }

    public void deleteApiVm(VmwareVirtualMachineInfo vmInfo)
            throws IOException {
        if (vmInfo == null || vmInfo.apiVm == null) {
            s_logger.error("Cannot delete VM: null arguments");
            throw new IllegalArgumentException("Null arguments");
        }

        apiConnector.delete(vmInfo.apiVm);
        vmInfo.apiVm = null;
        s_logger.info("Deleted " + vmInfo.apiVm);
    }

    public void createOrUpdateApiVmi(VmwareVirtualMachineInterfaceInfo vmiInfo)
            throws IOException {
        VmwareVirtualMachineInfo vmInfo = vmiInfo.vmInfo;
        VirtualMachine vm = vmInfo.apiVm;
        VirtualNetwork network = vmiInfo.vnInfo.apiVn;
        String descr = "VMI <" + vmiInfo.vnInfo.getName() + ", "
                + vmInfo.getName() + ">";
        
        // find VMI matching vmUuid & vnUuid
        List<ObjectReference<ApiPropertyBase>> vmInterfaceRefs =
                vm.getVirtualMachineInterfaceBackRefs();
        for (ObjectReference<ApiPropertyBase> vmInterfaceRef :
            Utils.safe(vmInterfaceRefs)) {
            String vmInterfaceUuid = vmInterfaceRef.getUuid();
            VirtualMachineInterface vmInterface = (VirtualMachineInterface)
                    apiConnector.findById(VirtualMachineInterface.class,
                            vmInterfaceUuid);
            List<ObjectReference<ApiPropertyBase>> vnRefs =
                                            vmInterface.getVirtualNetwork();
            for (ObjectReference<ApiPropertyBase> vnRef : vnRefs) {
                if (vnRef.getUuid().equals(network.getUuid())) {
                    s_logger.debug("VMI exits with vnUuid =" + network.getUuid()
                                 + " vmUuid = " + network.getUuid()
                                 + " no need to create new ");
                    vmiInfo.apiVmi = vmInterface;
                    return;
                }
            }
        }

        // create Virtual machine interface
        String vmInterfaceName = "vmi-" + network.getName()
                + "-" + vm.getName();
       
        VirtualMachineInterface vmInterface = new VirtualMachineInterface();
        vmInterface.setDisplayName(vmInterfaceName);
        vmInterface.setUuid(vmiInfo.getUuid());
        vmInterface.setName(vmiInfo.getUuid());
        vmInterface.setParent(vCenterProject);
        vmInterface.setSecurityGroup(vCenterDefSecGrp);
        vmInterface.setVirtualNetwork(network);
        vmInterface.addVirtualMachine(vm);
        MacAddressesType macAddrType = new MacAddressesType();
        macAddrType.addMacAddress(vmInfo.getMacAddress());
        vmInterface.setMacAddresses(macAddrType);
        vmInterface.setIdPerms(vCenterIdPerms);
        apiConnector.create(vmInterface);
        apiConnector.read(vmInterface);
        vmiInfo.apiVmi = vmInterface;
        s_logger.debug("Created " + descr + " virtual machine interface:" + vmInterfaceName
                + ", vmiUuid:" + vmiInfo.getUuid());
    }

    public void deleteApiVmi(VmwareVirtualMachineInterfaceInfo vmiInfo)
            throws IOException {
        if (vmiInfo == null || vmiInfo.apiVmi == null) {
            s_logger.error("Cannot delete VMI: null argument");
            throw new IllegalArgumentException("Null arguments");
        }
        apiConnector.delete(vmiInfo.apiVmi);
        vmiInfo.apiVmi = null;
        s_logger.info("Deleted " + vmiInfo.apiVmi);
    }

    public void createOrUpdateApiInstanceIp(
            VmwareVirtualMachineInterfaceInfo vmiInfo)
            throws IOException {
        VirtualNetwork network = vmiInfo.vnInfo.apiVn;
        VirtualMachine vm = vmiInfo.vmInfo.apiVm;
        VirtualMachineInterface vmIntf = vmiInfo.apiVmi;
        String vmIpAddress = "0.0.0.0";
        String instanceIpName = "ip-" + network.getName() + "-" + vmiInfo.vmInfo.getName() ;
        String instIpUuid = UUID.randomUUID().toString();
        
        InstanceIp instanceIp = new InstanceIp();
        instanceIp.setDisplayName(instanceIpName);
        instanceIp.setUuid(instIpUuid);
        instanceIp.setName(instIpUuid);
        instanceIp.setVirtualNetwork(network);
        instanceIp.setVirtualMachineInterface(vmIntf);
        instanceIp.setIdPerms(vCenterIdPerms);
        apiConnector.create(instanceIp);
        apiConnector.read(instanceIp);

        vmiInfo.apiInstanceIp = instanceIp;
        vmiInfo.setIpAddress(instanceIp.getAddress());
        s_logger.debug("Created instanceIP:" + instanceIp.getName() + ": " +
                vmIpAddress);
    }

    public void deleteApiInstanceIp(
            VmwareVirtualMachineInterfaceInfo vmiInfo)
            throws IOException {
        
        if (vmiInfo == null || vmiInfo.apiInstanceIp == null) {
            s_logger.info("Cannot delete Instance IP null object ");
            return;
        }

        apiConnector.delete(vmiInfo.apiInstanceIp);
        vmiInfo.apiInstanceIp = null;
        s_logger.info("Deleted " + vmiInfo.apiInstanceIp);
    }
    
    SortedMap<String, VirtualNetwork> readVirtualNetworks() {
        SortedMap<String, VirtualNetwork>  apiMap = 
                new TreeMap<String, VirtualNetwork>();
        
        List<VirtualNetwork> apiObjs = null;
        try {
            apiObjs = (List<VirtualNetwork>) 
                    apiConnector.list(VirtualNetwork.class, null);
        } catch (Exception ex) {
            s_logger.error("Exception in api.list(VirtualNetorks): " + ex);
            ex.printStackTrace();
            return apiMap;
        }
        
        for (VirtualNetwork obj : apiObjs) {
            try {
                // Read in the virtual network
                apiConnector.read(obj);
               // Ignore network ?
                if (doIgnoreVirtualNetwork(obj.getName())) {
                    continue;
                }
                // Ignore objects where creator isn't "vcenter-plugin"
                if ((obj.getIdPerms().getCreator() == null)  ||
                    !(obj.getIdPerms().getCreator().equals(VNC_VCENTER_PLUGIN))) {
                    continue;
                }
                apiMap.put(obj.getUuid(), obj);
                
            } catch (Exception e) {
                s_logger.error("Cannot read VN " + obj.getName());
            }
        }
        
        return apiMap;
    }

    void syncVirtualNetworks(SortedMap<String, VmwareVirtualNetworkInfo> vnInfoMap) {
        
        SortedMap<String, VirtualNetwork> apiMap = readVirtualNetworks();
        
        Iterator<Entry<String, VmwareVirtualNetworkInfo>> iter1 = vnInfoMap.entrySet().iterator();
        Iterator<Entry<String, VirtualNetwork>> iter2 = apiMap.entrySet().iterator();
        
        while (iter1.hasNext() && iter2.hasNext()) {
            Entry<String, VmwareVirtualNetworkInfo> entry1 = iter1.next();
            Entry<String, VirtualNetwork> entry2 = iter2.next();
              
            Integer cmp = entry1.getKey().compareTo(entry2.getKey());
            try {
                if (cmp <= 0) {
                    createOrUpdateVnApiObjects(entry1.getValue(),
                            entry2.getValue());
                } else {
                    apiConnector.delete(entry2.getValue());
                    //TODO delete all refs to this VM
                }
            } catch (Exception e) {
                s_logger.error("Cannot sync VN " + entry1.getKey());
            }
        }
        
        while (iter2.hasNext()) {
            Entry<String, VirtualNetwork> entry2 = iter2.next();
            try {
               //TODO delete all refs to this VM
                apiConnector.delete(entry2.getValue());
            } catch (Exception e) {
                s_logger.error("Cannot delete VN " + entry2.getKey());
            }
        }
        
        while (iter1.hasNext()) {
            Entry<String, VmwareVirtualNetworkInfo> entry1 = iter1.next();
            try {
                createOrUpdateVnApiObjects(entry1.getValue());
            } catch (Exception e) {
                s_logger.error("Cannot create VN " + entry1.getKey());
            }
        }
    }

    SortedMap<String, VirtualMachine> readVirtualMachines() {
        
        List<VirtualMachine> apiVms = null;
        SortedMap<String, VirtualMachine>  apiMap = 
                new TreeMap<String, VirtualMachine>();
        
        try {
            apiVms = (List<VirtualMachine>) 
                    apiConnector.list(VirtualMachine.class, null);
        } catch (Exception e) {
            s_logger.error("Exception in api.list(VirtualMachine): " + e);
            e.printStackTrace();
        }
       
        for (VirtualMachine vm : apiVms) {
            try {
                apiConnector.read(vm);
              
                // Ignore objects where creator isn't "vcenter-plugin"
                if ((vm.getIdPerms().getCreator() == null)  ||
                    !(vm.getIdPerms().getCreator().equals(VNC_VCENTER_PLUGIN))) {
                    continue;
                }
                
                apiMap.put(vm.getUuid(), vm);
            } catch (Exception e) {
                s_logger.error("Cannot sync VM " + vm.getName());
            }
        }
        
        return apiMap;
    }

    void syncVirtualMachines(SortedMap<String, VmwareVirtualMachineInfo> vmInfoMap) {
        
        Map<String, VirtualMachine> apiMap = readVirtualMachines();
        
        Iterator<Entry<String, VmwareVirtualMachineInfo>> iter1 = vmInfoMap.entrySet().iterator();
        Iterator<Entry<String, VirtualMachine>> iter2 = apiMap.entrySet().iterator();
        
        while (iter1.hasNext() && iter2.hasNext()) {
            Entry<String, VmwareVirtualMachineInfo> entry1 = iter1.next();
            Entry<String, VirtualMachine> entry2 = iter2.next();
            
            Integer cmp = entry1.getKey().compareTo(entry2.getKey());
            try {
                if (cmp <= 0) {
                    createOrUpdateVmApiObjects(entry1.getValue(), entry2.getValue());
                } else {
                    apiConnector.delete(entry2.getValue());
                    //TODO delete all refs to this VM
                }
            } catch (Exception e) {
                s_logger.error("Cannot sync VM " + entry1.getKey());
            }
        }
        
        while (iter2.hasNext()) {
            Entry<String, VirtualMachine> entry2 = iter2.next();
            try {
               //TODO delete all refs to this VM
                apiConnector.delete(entry2.getValue());
            } catch (Exception e) {
                s_logger.error("Cannot delete VM " + entry2.getKey());
            }
        }
        
        while (iter1.hasNext()) {
            Entry<String, VmwareVirtualMachineInfo> entry1 = iter1.next();
            try {
                createOrUpdateVmApiObjects(entry1.getValue());
            } catch (Exception e) {
                s_logger.error("Cannot create VM " + entry1.getKey()); 
            }
        }
    }

    public void init() throws Exception {
        while (Initialize() != true) {
            Thread.sleep(2);
        }
    }
}
