/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */

package net.juniper.contrail.vcenter;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.UUID;
import net.juniper.contrail.contrail_vrouter_api.ContrailVRouterApi;

public class VRouterNotifier {
    static volatile HashMap<String, ContrailVRouterApi> vrouterApiMap = 
            new HashMap<String, ContrailVRouterApi>();
    static final int vrouterApiPort = 9090;

    public static void addPort(VmwareVirtualMachineInterfaceInfo vmiInfo) {

        if (vmiInfo == null || vmiInfo.apiVmi == null || vmiInfo.apiInstanceIp == null
                || vmiInfo.vmInfo == null || vmiInfo.vmInfo.apiVm == null 
                || vmiInfo.vnInfo == null || vmiInfo.vnInfo.apiVn == null) {
                
            throw new IllegalArgumentException("Null argument");
        }

        String vrouterIpAddress = vmiInfo.getVmInfo().getVrouterIpAddress();
        VmwareVirtualMachineInfo vmInfo = vmiInfo.vmInfo;
        VmwareVirtualNetworkInfo vnInfo = vmiInfo.vnInfo;
        
        // Plug notification to vrouter
        if (vrouterIpAddress == null) {
            /*s_logger.warn("Virtual machine: " + vmName + " esxi host: " + hostName
                + " addPort notification NOT sent as vRouterIp Address not known");*/
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
            if (vmInfo.isPoweredOnState()) {
                String ipAddr = vmiInfo.getIpAddress();
                if (ipAddr == null) {
                    ipAddr = "0.0.0.0";
                }
                boolean ret = vrouterApi.AddPort(UUID.fromString(vmiInfo.getUuid()),
                        UUID.fromString(vmInfo.getUuid()), vmiInfo.getUuid(),
                        InetAddress.getByName(ipAddr),
                        Utils.parseMacAddress(vmiInfo.getMacAddress()),
                        UUID.fromString(vnInfo.getUuid()),
                        vnInfo.getIsolatedVlanId(),
                        vnInfo.getPrimaryVlanId(), vmInfo.getName());
                if ( ret == true) {
                    /*
                    s_logger.info("VRouterAPi Add Port success - interface name:"
                                  +  vmInterface.getDisplayName()
                                  + "(" + vmInterface.getName() + ")"
                                  + ", VM=" + vmName
                                  + ", VN=" + network.getName()
                                  + ", vmIpAddress=" + vmIpAddress
                                  + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
                     */
                } else {
                    // log failure but don't worry. Periodic KeepAlive task will
                    // attempt to connect to vRouter Agent and replay AddPorts.
                    /*
                    s_logger.error("VRouterAPi Add Port failed - interface name: "
                                  +  vmInterface.getDisplayName()
                                  + "(" + vmInterface.getName() + ")"
                                  + ", VM=" + vmName
                                  + ", VN=" + network.getName()
                                  + ", vmIpAddress=" + vmIpAddress
                                  + ", vlan=" + primaryVlanId + "/" + isolatedVlanId);
                     */
                }
            } else {
                //s_logger.info("VM (" + vmName + ") is PoweredOff. Skip AddPort now.");
            }
        }catch(Throwable e) {
            //s_logger.error("Exception : " + e);
            e.printStackTrace();
        }
    }

    public static void deletePort(VmwareVirtualMachineInterfaceInfo vmiInfo) {
    }
}
