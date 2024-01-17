package org.dcache.simplenfs;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.nio.file.Path;
import org.dcache.nfs.ExportFile;
import org.dcache.oncrpc4j.portmap.OncRpcEmbeddedPortmap;

/**
 *
 */
public class App {

    @Option(name = "-root", usage = "root of the file system to export", metaVar = "<path>")
    private Path root;
    @Option(name = "-exports", usage = "path to file with export tables", metaVar = "<file>")
    private Path exportsFile;
    @Option(name = "-nfsvers", usage = "NFS version (3, 4, 0==3+4) to use", metaVar = "<int>")
    private int nfsVers = 0;
    @Option(name = "-port", usage = "TCP port to use", metaVar = "<port>")
    private int rpcPort = 2049;
    @Option(name = "-with-portmap", usage = "start embedded portmap")
    private boolean withPortmap;

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    public void run(String[] args) throws CmdLineException, IOException {

        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println();
            System.err.println(e.getMessage());
            System.err.println("Usage:");
            System.err.println("    App [options...]");
            System.err.println();
            parser.printUsage(System.err);
            System.exit(1);
        }

        ExportFile exportFile = null;
        if (exportsFile != null) {
            exportFile = new ExportFile(exportsFile.toFile());
        }

        if (withPortmap) {
            new OncRpcEmbeddedPortmap();
        }

        try (SimpleNfsServer ignored = new SimpleNfsServer(nfsVers, rpcPort, root, exportFile, null)) {
            //noinspection ResultOfMethodCallIgnored
            System.in.read(); //any key to shutdown
        }
    }
}
