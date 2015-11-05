/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import com.vmware.vim25.DVPortgroupCreatedEvent;
import com.vmware.vim25.DVPortgroupDestroyedEvent;
import com.vmware.vim25.DVPortgroupReconfiguredEvent;
import com.vmware.vim25.DVPortgroupRenamedEvent;
import com.vmware.vim25.Event;
import com.vmware.vim25.VmBeingCreatedEvent;
import com.vmware.vim25.VmCloneEvent;
import com.vmware.vim25.VmClonedEvent;
import com.vmware.vim25.VmCreatedEvent;
import com.vmware.vim25.VmDeployedEvent;
import com.vmware.vim25.VmMacAssignedEvent;
import com.vmware.vim25.VmMacChangedEvent;
import com.vmware.vim25.VmPoweredOffEvent;
import com.vmware.vim25.VmPoweredOnEvent;
import com.vmware.vim25.VmReconfiguredEvent;
import com.vmware.vim25.VmRemovedEvent;
import com.vmware.vim25.VmRenamedEvent;

public class VCenterEventHandler implements Runnable {
    Event event;
    VCenterDB vcenterDB;
    VncDB vncDB;
    EventData vcenterEvent;

    VCenterEventHandler(Event event, VCenterDB vcenterDB, VncDB vncDB) {
        this.event = event;
        this.vcenterDB = vcenterDB;
        this.vncDB = vncDB;
    }

    private void printEvent() {
        /*
        s_logger.info("===============");
        s_logger.info("\nEvent Details follows:");

        s_logger.info("\n----------" + "\n Event ID: "
                + evt.getKey() + "\n Event: "
                + evt.getClass().getName()
                + "\n FullFormattedMessage: "
                + evt.getFullFormattedMessage()
                + "\n----------\n");
         */
    }

    @Override
    public void run() {
        printEvent();

        vcenterDB.printInfo();
        
        try {
            vcenterEvent = new EventData(event, vcenterDB, vncDB);

            if (event instanceof VmBeingCreatedEvent
                || event instanceof VmCreatedEvent
                || event instanceof VmClonedEvent
                || event instanceof VmCloneEvent
                || event instanceof VmDeployedEvent) {
                handleVmCreateEvent();
            } else if (event instanceof VmReconfiguredEvent
                || event instanceof  VmRenamedEvent
                || event instanceof VmMacChangedEvent
                || event instanceof VmMacAssignedEvent
                || event instanceof VmPoweredOnEvent
                || event instanceof VmPoweredOffEvent) {
                handleVmUpdateEvent();
            } else if (event instanceof VmRemovedEvent) {
                handleVmDeleteEvent();
            } else if (event instanceof DVPortgroupCreatedEvent) {
                handleNetworkCreateEvent();
            } else if (event instanceof DVPortgroupReconfiguredEvent
                    || event instanceof DVPortgroupRenamedEvent) {
                handleNetworkUpdateEvent();
            } else if (event instanceof DVPortgroupDestroyedEvent) {
                handleNetworkDeleteEvent();
            } else {
                handleEvent(event);
            }
        } catch (IOException e) {
            // log unable to process event;
            // add this event to the retry queue
            return;
        } catch (Exception e) {
            // log unable to process event;
            // add this event to the retry queue
            return;
        }
        
        vcenterDB.printInfo();
    }

    private void watchVm() {
        //TODO as per CacheFrameworkSample
    }

    private void handleVmCreateEvent() throws IOException {
        if (vcenterEvent.vrouterIpAddress == null) {
            /*log ("ContrailVM not found on ESXi host: "
                    + vcenterEvent.host.getName() + ", skipping VM (" +
                    vcenterEvent.vm.getName() + ") creation"
                    + " on network: " + vcenterEvent.nw.getName());*/
            return;
        }

        handleVmUpdateEvent();
        
        // add a watch on this Vm guest OS to be notified of further changes
        watchVm();
    }

    private void handleVmUpdateEvent() throws IOException {
        if (vcenterEvent.vrouterIpAddress == null) {
            /*log ("ContrailVM not found on ESXi host: "
                    + vcenterEvent.host.getName() + ", skipping VM (" +
                    vcenterEvent.vm.getName() + ") creation"
                    + " on network: " + vcenterEvent.nw.getName());*/
            return;
        }

        VmwareVirtualMachineInfo vmInfo = vcenterEvent.vmInfo;
        VmwareVirtualMachineInfo oldVmInfo = vcenterDB.getVmById(vmInfo.getUuid());
       
        if (vmInfo.equals(oldVmInfo)) {
            // nothing changed
            return;
        }
 
        vncDB.createOrUpdateApiVm(vmInfo);
 
        boolean updateVrouterNeeded = vmInfo.updateVrouterNeeded(oldVmInfo);
               
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter1 =
                vmInfo.getVmiInfo().entrySet().iterator();
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter2 =
                oldVmInfo.getVmiInfo().entrySet().iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter1.next();
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            Entry<String, VmwareVirtualMachineInterfaceInfo> oldEntry = iter2.next();
            VmwareVirtualMachineInterfaceInfo oldVmiInfo = oldEntry.getValue();
            
            if (updateVrouterNeeded || !entry.getKey().equals(oldEntry.getKey())
                    || !vmiInfo.equals(oldVmiInfo)) {
      
                // something has changed for this adaptor
                VRouterNotifier.deletePort(oldVmiInfo);
                if (oldVmiInfo.vnInfo.getExternalIpam() == false) {
                    vncDB.deleteApiInstanceIp(oldVmiInfo);
                }
                vncDB.deleteApiVmi(oldVmiInfo);
                
                vncDB.createOrUpdateApiVmi(vmiInfo);
                if (vmiInfo.vnInfo.getExternalIpam() == false) {
                    vncDB.createOrUpdateApiInstanceIp(vmiInfo);
                }
                VRouterNotifier.addPort(vmiInfo);
            }
        }
        while (iter2.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> oldEntry = iter2.next();
            VmwareVirtualMachineInterfaceInfo oldVmiInfo = oldEntry.getValue();
            VRouterNotifier.deletePort(oldVmiInfo);
            if (oldVmiInfo.vnInfo.getExternalIpam() == false) {
                vncDB.deleteApiInstanceIp(oldVmiInfo);
            }
            vncDB.deleteApiVmi(oldVmiInfo);
        }
        
        while (iter1.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter1.next();
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            
            vncDB.createOrUpdateApiVmi(vmiInfo);
            if (vmiInfo.vnInfo.getExternalIpam() == false) {
                vncDB.createOrUpdateApiInstanceIp(vmiInfo);
            }
            VRouterNotifier.addPort(entry.getValue());
        }
        
        // vmInfo will not become current
        vcenterDB.updateVM(vmInfo);
    }

    private void handleVmDeleteEvent() throws IOException {
        if (vcenterEvent.vrouterIpAddress == null) {
            /*log ("ContrailVM not found on ESXi host: "
                    + vcenterEvent.host.getName() + ", skipping VM (" +
                    vcenterEvent.vm.getName() + ") creation"
                    + " on network: " + vcenterEvent.nw.getName());*/
            return;
        }

        VmwareVirtualMachineInfo vmInfo = vcenterEvent.vmInfo;
        
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter =
                vmInfo.getVmiInfo().entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter.next();
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            VRouterNotifier.deletePort(vmiInfo);
        }
        
        vncDB.deleteVmApiObjects(vmInfo);
        
        vcenterDB.deleteVM(vmInfo);
    }

    private void handleNetworkCreateEvent() throws IOException {
        handleNetworkUpdateEvent();
    }

    private void handleNetworkUpdateEvent() throws IOException {
        VmwareVirtualNetworkInfo oldVnInfo = vcenterDB.getVnByName(vcenterEvent.vnInfo.getName());
        if (vcenterEvent.vnInfo.equals(oldVnInfo)) {
            // nothing changed
            return;
        }

        vncDB.createOrUpdateVnApiObjects(vcenterEvent.vnInfo);
        vcenterDB.updateVN(vcenterEvent.vnInfo);
        
        //TODO what kind of network updates need to be send to the vrouter?
    }

    private void handleNetworkDeleteEvent() throws IOException {
        vncDB.deleteVnApiObjects(vcenterEvent.vnInfo);
        
        vcenterDB.deleteVN(vcenterEvent.vnInfo);
    }

    private void handleEvent(Event event) throws IOException {
        throw new UnsupportedOperationException("Buddy you need to get a hold off this event");
    }
}
