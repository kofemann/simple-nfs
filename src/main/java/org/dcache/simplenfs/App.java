/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.simplenfs;

import java.io.File;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.NfsServerV3;
import org.dcache.chimera.nfs.v4.DeviceManager;
import org.dcache.chimera.nfs.v4.MDSOperationFactory;
import org.dcache.chimera.nfs.v4.NFSServerV41;
import org.dcache.chimera.nfs.v4.SimpleIdMap;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.chimera.posix.UnixPermissionHandler;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;

/**
 *
 * @author tigran
 */
public class App {
    public static void main(String[] args) throws Exception {


        VirtualFileSystem vfs = new LocalFileSystem( new File("/home/tigran"));
        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
                .withPort(2049)
                .withTCP()
                .withAutoPublish()
                .build();

        ExportFile exportFile = new ExportFile(new File("/etc/exports"));
        NFSServerV41 nfs4 = new NFSServerV41(
                new MDSOperationFactory(),
                new DeviceManager(),
                UnixPermissionHandler.getInstance(),
                vfs,
                new SimpleIdMap(), exportFile);

        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);

        nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
        nfsSvc.register(new OncRpcProgram(100005, 3), mountd);

        nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
        nfsSvc.start();

        System.in.read();
    }   
}
