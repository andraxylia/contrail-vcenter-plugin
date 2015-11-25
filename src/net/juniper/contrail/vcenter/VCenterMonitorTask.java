/**
 * Copyright (c) 2014 Juniper Networks, Inc. All rights reserved.
 */

package net.juniper.contrail.vcenter;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import com.google.common.base.Throwables;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import net.juniper.contrail.api.types.VirtualMachine;
import net.juniper.contrail.api.types.VirtualMachineInterface;
import net.juniper.contrail.watchdog.TaskWatchDog;

class VCenterMonitorTask implements Runnable {
    private static Logger s_logger = Logger.getLogger(VCenterMonitorTask.class);
    private VCenterDB vcenterDB;
    private VncDB vncDB;
    private Mode mode;
    private boolean AddPortSyncAtPluginStart = true;
    private boolean VncDBInitCompelete = false;
    private boolean VcenterDBInitComplete = false;
    public boolean VCenterNotifyForceRefresh = false;
    static volatile boolean syncNeeded = true;

    public VCenterMonitorTask(VCenterDB vcenterDB, VncDB vncDB, Mode mode) {
        this.vcenterDB = vcenterDB; 
        this.vncDB     = vncDB;
        this.mode      = mode;
    }

    public void Initialize() {
        // Initialize the databases
        if (vncDB.Initialize() == true) {
            VncDBInitCompelete = true;
        }
        if (vcenterDB.Initialize() == true && vcenterDB.Initialize_data() == true) {
            VcenterDBInitComplete = true;
        }
    }

    public boolean getVCenterNotifyForceRefresh() {
         return VCenterNotifyForceRefresh;
    }

    public void setVCenterNotifyForceRefresh(boolean _VCenterNotifyForceRefresh) {
        VCenterNotifyForceRefresh = _VCenterNotifyForceRefresh;
    }

    public void setAddPortSyncAtPluginStart(boolean _AddPortSyncAtPluginStart)
    {
        AddPortSyncAtPluginStart = _AddPortSyncAtPluginStart;
    }

    public boolean getAddPortSyncAtPluginStart()
    {
        return AddPortSyncAtPluginStart;
    }


    @Override
    public void run() {
        
        //check if you are the master from time to time
        //sometimes things dont go as planned
        if (VCenterMonitor.isZookeeperLeader() == false) {
            s_logger.debug("Lost zookeeper leadership. Restarting myself\n");
            System.exit(0);
        }

        if (VncDBInitCompelete == false) {
            initVncConnection();
        }
        if (VcenterDBInitComplete == false) {
            initVcenterConnection();
        }
        
        checkVroutersConnection();

        // Perform sync between VNC and VCenter DBs.
        if (getAddPortSyncAtPluginStart() == true || syncNeeded) {
            TaskWatchDog.startMonitoring(this, "Sync",
                    300000, TimeUnit.MILLISECONDS);

            // When syncVirtualNetworks is run the first time, it also does
            // addPort to vrouter agent for existing VMIs.
            // Clear the flag  on first run of syncVirtualNetworks.
            try {
                MainDB.sync(vcenterDB, vncDB, mode);
                syncNeeded = false;
                setAddPortSyncAtPluginStart(false);
            } catch (Exception e) {
                String stackTrace = Throwables.getStackTraceAsString(e);
                s_logger.error("Error in initial sync: " + e); 
                s_logger.error(stackTrace);
                e.printStackTrace();
                if (stackTrace.contains("java.net.ConnectException: Connection refused") ||
                    stackTrace.contains("java.rmi.RemoteException: VI SDK invoke"))   {
                        //Remote Exception. Some issue with connection to vcenter-server
                        // Exception on accessing remote objects.
                        // Try to reinitialize the VCenter connection.
                        //For some reason RemoteException not thrown
                        s_logger.error("Problem with connection to vCenter-Server");
                        s_logger.error("Restart connection and reSync");
                        vcenterDB.connectRetry();
                        this.VCenterNotifyForceRefresh = true;
                }
            }
            TaskWatchDog.stopMonitoring(this);
        }
    }

    private void initVncConnection() {
        TaskWatchDog.startMonitoring(this, "Init Vnc", 
                300000, TimeUnit.MILLISECONDS);   
        try {
            if (vncDB.Initialize() == true) {
                VncDBInitCompelete = true;
            }
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error("Error while initializing Vnc connection: " + e); 
            s_logger.error(stackTrace); 
            e.printStackTrace();
        }
        TaskWatchDog.stopMonitoring(this);
    }

    private void initVcenterConnection() {       
        TaskWatchDog.startMonitoring(this, "Init VCenter", 
                300000, TimeUnit.MILLISECONDS);
        try {
            if (vcenterDB.Initialize() == true && vcenterDB.Initialize_data() == true) {
                VcenterDBInitComplete = true;
            } 
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error("Error while initializing VCenter connection: " + e); 
            s_logger.error(stackTrace); 
            e.printStackTrace();
        }
        TaskWatchDog.stopMonitoring(this);
    }

    private void checkVroutersConnection() {
        TaskWatchDog.startMonitoring(this, "VRouter Keep alive check",
                60000, TimeUnit.MILLISECONDS);
        // run KeepAlive with vRouter Agent.

        try {
            vncDB.vrouterAgentPeriodicConnectionCheck(vcenterDB.vRouterActiveMap);
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            s_logger.error("Error while vrouterAgentPeriodicConnectionCheck: " + e); 
            s_logger.error(stackTrace); 
            e.printStackTrace();
        }

        TaskWatchDog.stopMonitoring(this);
    }
}
