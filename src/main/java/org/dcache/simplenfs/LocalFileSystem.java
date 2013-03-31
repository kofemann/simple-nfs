package org.dcache.simplenfs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.vfs.DirectoryEntry;
import org.dcache.chimera.nfs.vfs.FsStat;
import org.dcache.chimera.nfs.vfs.Inode;
import org.dcache.chimera.nfs.vfs.Inode.Type;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.chimera.posix.Stat;
import org.dcache.utils.Bytes;

/**
 *
 */
public class LocalFileSystem implements VirtualFileSystem {

    private final Path _root;
    private ConcurrentMap<Integer, LocalInode> _id_cache = new ConcurrentHashMap<>();
    private final Cache<LocalInode, FileChannel> _openFiles;

    public LocalFileSystem(File root) {
        _root = root.toPath();
        _openFiles = CacheBuilder
                .newBuilder()
                .removalListener( new LocalFileCloser())
                .build( new LocalFileOpener());
    }

    @Override
    public Inode create(Inode parent, Type type, String path, int uid, int gid, int mode) throws IOException {
        LocalInode localInode = (LocalInode) parent;
        Path parentPath = localInode.getPath();
        Path newPath = parentPath.resolve(path);
        Files.createFile(newPath);
        return new LocalInode(newPath);
    }

    @Override
    public FsStat getFsStat() throws IOException {

        File fileStore = _root.toFile();
        long totalSpace = fileStore.getTotalSpace();
        long avail = fileStore.getUsableSpace();
        long totalFiles = 0;
        long usedFiles = 0;
        
        return new FsStat(totalSpace, totalFiles, totalSpace - avail, usedFiles);
    }

    @Override
    public Inode getRootInode() throws IOException {
        return new LocalInode(_root);
    }

    @Override
    public Inode inodeOf(byte[] fh) throws IOException {
        int id = Bytes.getInt(fh, 0);
        return _id_cache.get(id);
    }

    @Override
    public Inode inodeOf(Inode parent, String path) throws IOException {
        LocalInode localInode = (LocalInode) parent;
        Path parentPath = localInode.getPath();

        Path element = parentPath.resolve(path);
        if (!Files.exists(element)) {
            throw new ChimeraNFSException(nfsstat.NFSERR_NOENT, element.toString());
        }
        return new LocalInode(element);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, int uid, int gid) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        LocalInode localInode = (LocalInode) inode;
        Path path = localInode.getPath();
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);
        List<DirectoryEntry> list = new ArrayList<>();
        for (Path p : directoryStream) {
            list.add(new DirectoryEntry(p.getFileName().toString(), new LocalInode(p)));
        }
        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, int uid, int gid, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        LocalInode localInode = (LocalInode) inode;

        FileChannel fc = _openFiles.getUnchecked(localInode);
        ByteBuffer b = ByteBuffer.wrap(data);
        b.limit(count);
        return fc.read(b, offset);
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        LocalInode localInode = (LocalInode) inode;
        Path path = localInode.getPath();
        return Files.readSymbolicLink(path).toString();
    }

    @Override
    public boolean remove(Inode parent, String path) throws IOException {
        LocalInode parentInode = (LocalInode) parent;
        Path parentPath = parentInode.getPath();
        Files.delete(parentPath.resolve(path));
        return true;
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, int uid, int gid, int mode) throws IOException {
        LocalInode parentInode = (LocalInode) parent;
        Path parentPath = parentInode.getPath();

        Path pointingPath = parentPath.getFileSystem().getPath(link);
        Path linkPath = Files.createSymbolicLink(parentPath, pointingPath);
        return new LocalInode(linkPath);
    }

    @Override
    public int write(Inode inode, byte[] data, long offset, int count) throws IOException {
        LocalInode localInode = (LocalInode) inode;

        FileChannel fc = _openFiles.getUnchecked(localInode);
        ByteBuffer b = ByteBuffer.wrap(data);
        b.limit(count);
        return fc.write(b, offset);
    }

    private class LocalInode implements Inode {

        private final Path _path;
        private final PosixFileAttributes _attrs;

        public LocalInode(Path path) throws IOException {
            _path = path;
            _attrs = Files.getFileAttributeView(_path, PosixFileAttributeView.class).readAttributes();
        }

        @Override
        public byte[] toFileHandle() throws IOException {
            byte[] fh = new byte[4];
            int id = _path.hashCode();
            Bytes.putInt(fh, 0, _path.hashCode());
            _id_cache.put(id, this);
            return fh;
        }

        @Override
        public boolean exists() {
            return Files.exists(_path);
        }

        @Override
        public Stat stat() throws IOException {
            Stat stat = new Stat();
            stat.setATime(_attrs.lastAccessTime().toMillis());
            stat.setMTime(_attrs.lastModifiedTime().toMillis());
            stat.setCTime(_attrs.lastModifiedTime().toMillis());
            stat.setSize(_attrs.size());
            stat.setMode(toUnixMode(_attrs));
            return stat;
        }

        @Override
        public Stat statCache() throws IOException {
            return stat();
        }

        @Override
        public long id() {
            return hashCode() << 31 | _attrs.creationTime().toMillis();
        }

        @Override
        public void setSize(long size) throws IOException {
            
        }

        @Override
        public void setUID(int id) throws IOException {
            
        }

        @Override
        public void setGID(int id) throws IOException {
            
        }

        @Override
        public void setATime(long time) throws IOException {
            
        }

        @Override
        public void setMTime(long time) throws IOException {
            
        }

        @Override
        public void setCTime(long time) throws IOException {
            
        }

        @Override
        public void setMode(int size) throws IOException {
            
        }

        @Override
        public nfsace4[] getAcl() throws IOException {
            return new nfsace4[0];
        }

        @Override
        public void setAcl(nfsace4[] acl) throws IOException {
            
        }

        @Override
        public Type type() {
            if (_attrs.isRegularFile()) {
                return Type.REGULAR;
            } else if (_attrs.isDirectory()) {
                return Type.DIRECTORY;
            } else if (_attrs.isSymbolicLink()) {
                return Type.SYMLINK;
            } else {
                return Type.SOCK;
            }
        }

        Path getPath() {
            return _path;
        }

        private int toUnixMode(PosixFileAttributes attrs) {
            int mode = 0;
            for (PosixFilePermission perm : attrs.permissions()) {
                switch (perm) {
                    case OWNER_READ:
                        mode |= UnixPermission.S_IRUSR;
                        break;
                    case OWNER_WRITE:
                        mode |= UnixPermission.S_IWUSR;
                        break;
                    case OWNER_EXECUTE:
                        mode |= UnixPermission.S_IXUSR;
                        break;
                    case GROUP_READ:
                        mode |= UnixPermission.S_IRGRP;
                        break;
                    case GROUP_WRITE:
                        mode |= UnixPermission.S_IWGRP;
                        break;
                    case GROUP_EXECUTE:
                        mode |= UnixPermission.S_IXGRP;
                        break;
                    case OTHERS_READ:
                        mode |= UnixPermission.S_IROTH;
                        break;
                    case OTHERS_WRITE:
                        mode |= UnixPermission.S_IWOTH;
                        break;
                    case OTHERS_EXECUTE:
                        mode |= UnixPermission.S_IXOTH;
                        break;
                }
            }
            return (_attrs.isDirectory() ? 0040000 : 0100000) | mode;
        }
    }

    private class LocalFileOpener extends CacheLoader<LocalInode, FileChannel> {

        @Override
        public FileChannel load(LocalInode key) throws Exception {
            Path p = key.getPath();
            RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw");
            return raf.getChannel();
        }
    }

    private class LocalFileCloser implements RemovalListener<LocalInode, FileChannel> {

        @Override
        public void onRemoval(RemovalNotification<LocalInode, FileChannel> notification) {
            try {
                notification.getValue().close();
            } catch (IOException e) {
                // NOP
            }
        }
    }
}
