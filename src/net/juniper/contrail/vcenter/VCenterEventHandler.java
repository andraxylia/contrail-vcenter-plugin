/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 * 
 * Handles functionality related to events received from VCenter
 */
package net.juniper.contrail.vcenter;

import java.io.IOException;
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
        
        MainDB.updateVirtualMachine(vmInfo);
    }

    private void handleVmDeleteEvent() throws Exception {
        VmwareVirtualMachineInfo vmInfo = new VmwareVirtualMachineInfo(event, vcenterDB);
        
        if (vmInfo.ignore()) {
            return;
        }
  
        MainDB.deleteVirtualMachine(vmInfo);
    }

    private void handleNetworkCreateEvent() throws Exception {
        handleNetworkUpdateEvent();
    }

    private void handleNetworkUpdateEvent() throws Exception {
        VmwareVirtualNetworkInfo vnInfo = 
                new VmwareVirtualNetworkInfo(event, vcenterDB);
        
        if (vnInfo.ignore()) {
            return;
        }

        MainDB.updateVirtualNetwork(vnInfo);
    }

    private void handleNetworkDeleteEvent() throws Exception {
        VmwareVirtualNetworkInfo vnInfo = 
                new VmwareVirtualNetworkInfo(event, vcenterDB);
        
        if (vnInfo.ignore()) {
            return;
        }
 
        MainDB.deleteVirtualNetwork(vnInfo);
    }

    private void handleEvent(Event event) throws IOException {
        throw new UnsupportedOperationException("Buddy you need to get a hold off this event");
    }
}
