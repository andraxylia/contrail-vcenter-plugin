/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 *
 * @author Andra Cismaru
 */
package net.juniper.contrail.vcenter;

import java.io.IOException;
import com.vmware.vim25.Event;
import com.vmware.vim25.VmDeployedEvent;
import com.vmware.vim25.VmPoweredOffEvent;
import com.vmware.vim25.VmPoweredOnEvent;
import com.vmware.vim25.VmReconfiguredEvent;

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

            if (vcenterEvent.vrouterIpAddress == null) {
                /*log ("ContrailVM not found on ESXi host: "
                        + vcenterEvent.host.getName() + ", skipping VM (" +
                        vcenterEvent.vm.getName() + ") creation"
                        + " on network: " + vcenterEvent.nw.getName());*/
                return;
            }

            if (event instanceof VmReconfiguredEvent) {
                handleEvent((VmReconfiguredEvent)event);
            } else if (event instanceof VmDeployedEvent) {
                handleEvent((VmDeployedEvent)event);
            } else {
                handleEvent(event);
            }
        } catch (IOException e) {
            // log unable to process event;
            return;
        } catch (Exception e) {
            // log unable to process event;
            return;
        }
    }

    private void watchVm(EventData vcenterEvent) {
        //TODO as per sample code
    }

    public void handleEvent(VmDeployedEvent event) throws IOException {
        vcenterDB.addVM(vcenterEvent);

        vncDB.createApiServerVmObjects(vcenterEvent);
        watchVm(vcenterEvent);

        // if vmpowered on add port
        if (vcenterEvent.vmInfo.isPoweredOnState()) {
            VRouterNotifier.addPort(vcenterEvent);
        }
    }

    public void handleEvent(VmReconfiguredEvent event) {
        vcenterDB.updateVM(vcenterEvent);
        // if reconfigured triggered a change in new, ip address or mac
        VRouterNotifier.deletePort(vcenterEvent);
        VRouterNotifier.addPort(vcenterEvent);
    }

    public void handleEvent(VmPoweredOnEvent value) {
        // TODO if this is VCenter as a compute, do not do the below line
        vcenterDB.updateVM(vcenterEvent);
        // update vrouter addPort
        VRouterNotifier.addPort(vcenterEvent);
    }

    public void handleEvent(VmPoweredOffEvent value) {
        // TODO if this is VCenter as a compute, do not do the below line
        vcenterDB.updateVM(vcenterEvent);
        // update vrouter deletePort
        VRouterNotifier.deletePort(vcenterEvent);
    }

    public void handleEvent(Event event) {
        throw new UnsupportedOperationException("Buddy you need to get a hold off this event");
    }
}