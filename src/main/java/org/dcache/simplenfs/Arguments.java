package org.dcache.simplenfs;

import org.kohsuke.args4j.Option;

import java.nio.file.Path;

public class Arguments {
    @Option(name = "-root")
    private Path root = null;
    @Option(name = "-exportsFile")
    private Path exportsFile = null;
    @Option(name= "-port")
    private int rpcPort = 2049;
    @Option(name="-with-portmap")
    private boolean withPortmap;

    public Path getRoot() {
        return root;
    }

    public void setRoot(Path root) {
        this.root = root;
    }

    public Path getExportsFile() {
        return exportsFile;
    }

    public void setExportsFile(Path exportsFile) {
        this.exportsFile = exportsFile;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public boolean getWithPortmap() {
        return withPortmap;
    }

    public void setWithPortmap(boolean withPortmap) {
        this.withPortmap = withPortmap;
    }
}
