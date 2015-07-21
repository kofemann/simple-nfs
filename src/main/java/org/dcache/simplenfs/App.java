package org.dcache.simplenfs;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.DeviceManager;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.net.URL;

/**
 *
 * @author tigran
 */
public class App {

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        CmdLineParser parser = new CmdLineParser(arguments);
        try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            throw new IllegalArgumentException(e);
        }

        ExportFile exportFile;
        if (arguments.getExportsFile()!=null) {
            exportFile = new ExportFile(arguments.getExportsFile());
        } else {
            URL exportsUrl = App.class.getClassLoader().getResource("exports");
            exportFile = new ExportFile(exportsUrl);
        }

        VirtualFileSystem vfs = new LocalFileSystem(arguments.getRoot(), exportFile.getExports());

        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
                .withPort(arguments.getRpcPort())
                .withTCP()
                .withAutoPublish()
                .withWorkerThreadIoStrategy()
                .build();

        NFSServerV41 nfs4 = new NFSServerV41(
                new MDSOperationFactory(),
                new DeviceManager(),
                vfs,
                exportFile);

        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);

        nfsSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd);
        nfsSvc.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
        nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
        nfsSvc.start();

        System.in.read();
    }
}
