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

        MainDB.printInfo();
        
        try {
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
        
        MainDB.printInfo();
    }

    private void watchVm() {
        //TODO as per CacheFrameworkSample
    }

    private void handleVmCreateEvent() throws Exception {
        
        handleVmUpdateEvent();
        
        // add a watch on this Vm guest OS to be notified of further changes
        watchVm();
    }

    private void handleVmUpdateEvent() throws Exception {
        VmwareVirtualMachineInfo vmInfo = new VmwareVirtualMachineInfo(event, vcenterDB);
         
        if (vmInfo.ignore()) {
            return;
        } 
        
        VmwareVirtualMachineInfo oldVmInfo = MainDB.getVmById(vmInfo.getUuid());
       
        if (vmInfo.equals(oldVmInfo)) {
            // nothing changed
            return;
        }
 
        //TODO add here special handling for vmware bug where ipaddress gets nulled
        
        vncDB.createOrUpdateApiVm(vmInfo, null);
 
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
        MainDB.updateVM(vmInfo);
    }

    private void handleVmDeleteEvent() throws Exception {
        VmwareVirtualMachineInfo vmInfo = new VmwareVirtualMachineInfo(event, vcenterDB);
        
        if (vmInfo.ignore()) {
            return;
        }
  
        Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter =
                vmInfo.getVmiInfo().entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter.next();
            VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
            VRouterNotifier.deletePort(vmiInfo);
        }
        
        vncDB.deleteVmApiObjects(vmInfo);
        
        MainDB.deleteVM(vmInfo);
    }

    private void handleNetworkCreateEvent() throws Exception {
        handleNetworkUpdateEvent();
    }

    private void handleNetworkUpdateEvent() throws Exception {
        VmwareVirtualNetworkInfo vnInfo = new VmwareVirtualNetworkInfo(event, vcenterDB);
        
        if (vnInfo.ignore()) {
            return;
        }

        VmwareVirtualNetworkInfo oldVnInfo = MainDB.getVnByName(vnInfo.getName());
        if (vnInfo.equals(oldVnInfo)) {
            // nothing changed
            return;
        }

        vncDB.createOrUpdateVnApiObjects(vnInfo);
        MainDB.updateVN(vnInfo);
    }

    private void handleNetworkDeleteEvent() throws Exception {
        VmwareVirtualNetworkInfo vnInfo = new VmwareVirtualNetworkInfo(event, vcenterDB);
        
        if (vnInfo.ignore()) {
            return;
        }
        
        vncDB.deleteVnApiObjects(vnInfo);
        
        MainDB.deleteVN(vnInfo);
    }

    private void handleEvent(Event event) throws IOException {
        throw new UnsupportedOperationException("Buddy you need to get a hold off this event");
    }
}
