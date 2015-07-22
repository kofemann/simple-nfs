package org.dcache.simplenfs;

import org.dcache.nfs.ExportFile;
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

        try (SimpleNfsServer ignored = new SimpleNfsServer(arguments.getRpcPort(), arguments.getRoot(), exportFile)) {
            //noinspection ResultOfMethodCallIgnored
            System.in.read(); //any key to shutdown
        }
    }
}
