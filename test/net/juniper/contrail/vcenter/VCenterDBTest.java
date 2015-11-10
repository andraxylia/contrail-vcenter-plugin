/**
 * Copyright (c) 2015 Juniper Networks, Inc. All rights reserved.
 */

package net.juniper.contrail.vcenter;

import java.io.IOException;
import org.apache.log4j.Logger;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VCenterDBTest extends TestCase {
    private static final Logger s_logger =
        Logger.getLogger(VCenterDBTest.class);
    private static VCenterDB vcenterDB;

    @Before
    public void globalSetUp() throws IOException {
        // Setup VCenter object
        vcenterDB = new VCenterDB("https://10.20.30.40/sdk", "admin", "admin123",
                                   "unittest_dc", "unittest_dvs", "unittest_fabric_pg",
                                   "vcenter-only");
    }

    @Test
    public void testDoIgnoreVirtualMachine() throws IOException {
        assertTrue(vcenterDB.doIgnoreVirtualMachine("ContrailVM-xyz"));
        assertTrue(vcenterDB.doIgnoreVirtualMachine("abc-ContrailVM-xyz"));
        assertTrue(vcenterDB.doIgnoreVirtualMachine("abc-contrailvm-xyz"));
        assertFalse(vcenterDB.doIgnoreVirtualMachine("Tenent-VM"));
    }

    @Test
    public void testDoIgnoreVirtualNetwork() throws IOException {
        //assertTrue(vcenterDB.doIgnoreVirtualNetwork());
    }
}
