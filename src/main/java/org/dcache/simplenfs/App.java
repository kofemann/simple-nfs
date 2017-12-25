package org.dcache.simplenfs;

import org.dcache.nfs.ExportFile;
import org.dcache.xdr.portmap.OncRpcEmbeddedPortmap;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author tigran
 */
public class App {

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        CmdLineParser parser = new CmdLineParser(arguments);
        parser.parseArgument(args);

        ExportFile exportFile = null;
        if (arguments.getExportsFile()!=null) {
            exportFile = new ExportFile(arguments.getExportsFile().toFile());
        }

        if (arguments.getWithPortmap()) {
            new OncRpcEmbeddedPortmap();
        }

        try (SimpleNfsServer ignored = new SimpleNfsServer(arguments.getRpcPort(), arguments.getRoot(), exportFile, null)) {
            //noinspection ResultOfMethodCallIgnored
            System.in.read(); //any key to shutdown
        }
    }
}
