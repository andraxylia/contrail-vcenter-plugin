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

        vcenterDB.updateVM(vcenterEvent);
        
        if (!vcenterEvent.changed) {
            return;
        }
        VmwareVirtualMachineInfo vmInfo = vcenterEvent.vmInfo;
        vncDB.createOrUpdateVmApiObjects(vmInfo);
        
        // if reconfigured triggered a change in new, ip address or mac
        if (vcenterEvent.updateVrouterNeeded) {
            Iterator<Entry<String, VmwareVirtualMachineInterfaceInfo>> iter =
                    vmInfo.getVmiInfo().entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, VmwareVirtualMachineInterfaceInfo> entry = iter.next();
                VmwareVirtualMachineInterfaceInfo vmiInfo = entry.getValue();
                VRouterNotifier.deletePort(vmiInfo);
                VRouterNotifier.addPort(vmiInfo);
            }
        }
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
        
        vcenterDB.deleteVM(vcenterEvent);
    }

    private void handleNetworkCreateEvent() throws IOException {
        handleNetworkUpdateEvent();
    }

    private void handleNetworkUpdateEvent() throws IOException {
        vcenterDB.updateVN(vcenterEvent);
        if (!vcenterEvent.changed) {
            return;
        }
        vncDB.createOrUpdateVnApiObjects(vcenterEvent.vnInfo);
    }

    private void handleNetworkDeleteEvent() throws IOException {
        vncDB.deleteVnApiObjects(vcenterEvent.vnInfo);
        
        vcenterDB.deleteVN(vcenterEvent);
    }

    private void handleEvent(Event event) throws IOException {
        throw new UnsupportedOperationException("Buddy you need to get a hold off this event");
    }
}
