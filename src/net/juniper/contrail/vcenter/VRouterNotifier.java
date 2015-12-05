/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */

package net.juniper.contrail.vcenter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import net.juniper.contrail.contrail_vrouter_api.ContrailVRouterApi;

public class VRouterNotifier {
    static volatile HashMap<String, ContrailVRouterApi> vrouterApiMap = 
            new HashMap<String, ContrailVRouterApi>();
    static final int vrouterApiPort = 9090;
    
    private final static Logger s_logger =
            Logger.getLogger(VRouterNotifier.class);

    public static void created(VmwareVirtualMachineInterfaceInfo vmiInfo) {

        if (vmiInfo == null || vmiInfo.apiVmi == null
                || vmiInfo.vmInfo == null || vmiInfo.vmInfo.apiVm == null 
                || vmiInfo.vnInfo == null || vmiInfo.vnInfo.apiVn == null) {
                
            throw new IllegalArgumentException("Null argument");
        }

        String vrouterIpAddress = vmiInfo.getVmInfo().getVrouterIpAddress();
        String ipAddress = vmiInfo.getIpAddress();
        VmwareVirtualMachineInfo vmInfo = vmiInfo.vmInfo;
        VmwareVirtualNetworkInfo vnInfo = vmiInfo.vnInfo;
        
        if (vrouterIpAddress == null) {
            s_logger.warn(vmiInfo +
                " addPort notification NOT sent as vRouterIp Address not known");
            return;
        }
        if (ipAddress == null) {
            s_logger.warn(vmiInfo +
                " addPort notification NOT sent as IPAM external and IP Address not set or vmware Tools not installed");
            return;
        }
        if (!vmInfo.isPoweredOnState()) {
            s_logger.info(vmInfo + " is PoweredOff. Skip AddPort now.");
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
            
               
            boolean ret = vrouterApi.AddPort(UUID.fromString(vmiInfo.getUuid()),
                    UUID.fromString(vmInfo.getUuid()), vmiInfo.getUuid(),
                    InetAddress.getByName(ipAddress),
                    Utils.parseMacAddress(vmiInfo.getMacAddress()),
                    UUID.fromString(vnInfo.getUuid()),
                    vnInfo.getIsolatedVlanId(),
                    vnInfo.getPrimaryVlanId(), vmInfo.getName());
            if ( ret == true) {
                s_logger.info("VRouterAPi Add Port success for " + vmiInfo);
            } else {
                // log failure but don't worry. Periodic KeepAlive task will
                // attempt to connect to vRouter Agent and replay AddPorts.
                s_logger.error("VRouterAPI Add Port failed for " + vmiInfo);
            }
        } catch(Throwable e) {
            s_logger.error("Exception in addPort for " + vmiInfo + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void deleted(VmwareVirtualMachineInterfaceInfo vmiInfo) {
        if (vmiInfo == null || vmiInfo.apiVmi == null
                || vmiInfo.vmInfo == null || vmiInfo.vmInfo.apiVm == null 
                || vmiInfo.vnInfo == null || vmiInfo.vnInfo.apiVn == null) {
                
            s_logger.error("Null argument, cannot perform deletePort");
            return;
        }

        String vrouterIpAddress = vmiInfo.getVmInfo().getVrouterIpAddress();
        String ipAddress = vmiInfo.getIpAddress();
        VmwareVirtualMachineInfo vmInfo = vmiInfo.vmInfo;
        VmwareVirtualNetworkInfo vnInfo = vmiInfo.vnInfo;
        
        if (vrouterIpAddress == null) {
            s_logger.warn(vmiInfo +
                " deletePort notification NOT sent as vRouterIp Address not known");
            return;
        }
        if (ipAddress == null) {
            s_logger.warn(vmiInfo +
                " deletePort notification NOT sent as IPAM external and IP Address not set or vmware Tools not installed");
            return;
        }
        
        ContrailVRouterApi vrouterApi = vrouterApiMap.get(vrouterIpAddress);
        if (vrouterApi == null) {
            try {
            vrouterApi = new ContrailVRouterApi(
                    InetAddress.getByName(vrouterIpAddress), 
                    vrouterApiPort, false, 1000);
            } catch (UnknownHostException e) {
                // log error ("Incorrect vrouter address");
                s_logger.error("deletePort failed due to unknown vrouter " + vrouterIpAddress);
                return;
            }
            vrouterApiMap.put(vrouterIpAddress, vrouterApi);
        }
        vrouterApi.DeletePort(UUID.fromString(vmiInfo.getUuid()));
    }
}
