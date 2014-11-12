package org.dcache.simplenfs;

import org.kohsuke.args4j.Option;

import java.io.File;

public class Arguments {
    @Option(name = "-root", required = true)
    private File root;
    @Option(name = "-exportsFile")
    private File exportsFile = null;

    public File getRoot() {
        return root;
    }

    public void setRoot(File root) {
        this.root = root;
    }

    public File getExportsFile() {
        return exportsFile;
    }

    public void setExportsFile(File exportsFile) {
        this.exportsFile = exportsFile;
    }
}
