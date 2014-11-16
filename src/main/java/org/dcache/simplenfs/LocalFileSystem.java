package org.dcache.simplenfs;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.primitives.Longs;
import org.dcache.chimera.UnixPermission;
import org.dcache.nfs.FsExport;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class LocalFileSystem implements VirtualFileSystem {

    private final Path _root;
    private final BiMap<Path, Long> _id_cache = HashBiMap.create();
    private final AtomicLong fileId = new AtomicLong();
    private final NfsIdMapping _idMapper = new SimpleIdMap();

    private long getOrCreateId(Path path) {
        Long id = _id_cache.get(path);
        if (id == null) {
            id = fileId.getAndIncrement();
            _id_cache.put(path, id);
        }
        return id;
    }

    private Inode toFh(long id) {
        return Inode.forFile(Longs.toByteArray(id));
    }

    private Long toId(Inode inode) {
        return Longs.fromByteArray(inode.getFileId());
    }

    private Path resolve(Inode inode) {
        return _id_cache.inverse().get(toId(inode));
    }

    public LocalFileSystem(File root, Iterable<FsExport> exportIterable) {
        _root = root.toPath();
        assert (Files.exists(_root));
        try {
            for (FsExport export : exportIterable) {
                Path exportRootPath = new File(_root.toFile(), export.getPath()).toPath();
                if (!Files.exists(exportRootPath)) {
                    Files.createDirectories(exportRootPath);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        _id_cache.put(_root, fileId.getAndIncrement());
    }

    @Override
    public Inode create(Inode parent, Type type, String path, Subject subject, int mode) throws IOException {
        long parentId = Longs.fromByteArray(parent.getFileId());
        Path parentPath = _id_cache.inverse().get(parentId);
        Path newPath = parentPath.resolve(path);
        Files.createFile(newPath);
        long newId = fileId.getAndIncrement();
        _id_cache.put(newPath, newId);
        return toFh(newId);
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
        Long id = _id_cache.get(_root);
        return toFh(id);
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {

        Path parentPath = resolve(parent);

        Path element = parentPath.resolve(path);
        if (!Files.exists(element)) {
            throw new NoEntException(element.toString());
        }

        long id = getOrCreateId(element);
        return toFh(id);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        Path path = resolve(inode);
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);
        List<DirectoryEntry> list = new ArrayList<>();
        for (Path p : directoryStream) {
            list.add(new DirectoryEntry(p.getFileName().toString(), lookup(inode, p.toString()), statPath(p)));
        }
        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        long parentId = Longs.fromByteArray(parent.getFileId());
        Path parentPath = _id_cache.inverse().get(parentId);
        Path newPath = parentPath.resolve(path);
        Files.createDirectory(newPath);
        long newId = fileId.getAndIncrement();
        _id_cache.put(newPath, newId);
        return toFh(newId);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        long srcId = Longs.fromByteArray(src.getFileId());
        Path srcPath = _id_cache.inverse().get(srcId);
        if (!Files.exists(srcPath)) {
            throw new NoEntException();
        }
        long destId = Longs.fromByteArray(dest.getFileId());
        Path destPath = _id_cache.inverse().get(destId);
        if (!Files.exists(destPath)) {
            throw new NoEntException();
        }
        Path srcFile = srcPath.resolve(oldName);
        if (!Files.exists(srcFile)) {
            throw new NoEntException();
        }
        Long id =_id_cache.get(srcFile);
        Path destFile = destPath.resolve(newName);
        if (Files.exists(destFile)) {
            throw new ExistException();
        }
        Files.move(destPath, destFile, StandardCopyOption.ATOMIC_MOVE);
        _id_cache.remove(srcFile);
        _id_cache.put(destPath, id);
        return true;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        Path path = resolve(inode);
        if (path.equals(_root)) {
            throw new NoEntException("no parent");
        }
        Path parent = path.getParent();
        long id = getOrCreateId(parent);
        return toFh(id);
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        long srcId = Longs.fromByteArray(inode.getFileId());
        Path srcPath = _id_cache.inverse().get(srcId);
        if (!Files.exists(srcPath)) {
            throw new NoEntException();
        }
        ByteBuffer destBuffer = ByteBuffer.wrap(data, 0, count);
        try (FileChannel channel = FileChannel.open(srcPath, StandardOpenOption.READ)) {
            return channel.read(destBuffer, offset);
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        Path path = resolve(inode);
        return Files.readSymbolicLink(path).toString();
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        Path parentPath = resolve(parent);
        Files.delete(parentPath.resolve(path));
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        long srcId = Longs.fromByteArray(inode.getFileId());
        Path srcPath = _id_cache.inverse().get(srcId);
        if (!Files.exists(srcPath)) {
            throw new NoEntException();
        }
        ByteBuffer srcBuffer = ByteBuffer.wrap(data, 0, count);
        try (FileChannel channel = FileChannel.open(srcPath, StandardOpenOption.WRITE)) {
            int bytesWritten = channel.write(srcBuffer, offset);
            return new WriteResult(StabilityLevel.FILE_SYNC, bytesWritten);
        }
    }

    @Override
    public void commit(Inode inode, long l, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private Stat statPath(Path p) throws IOException {
        PosixFileAttributes attrs = Files.getFileAttributeView(p, PosixFileAttributeView.class).readAttributes();

        Stat stat = new Stat();

        stat.setATime(attrs.lastAccessTime().toMillis());
        stat.setCTime(attrs.lastModifiedTime().toMillis());
        stat.setMTime(attrs.lastModifiedTime().toMillis());

        // FIXME
        stat.setGid(0);
        stat.setUid(0);

        stat.setDev(17);
        stat.setIno(attrs.fileKey().hashCode());
        stat.setMode(toUnixMode(attrs));
        stat.setNlink(1);
        stat.setRdev(17);
        stat.setSize(attrs.size());
        stat.setFileid(attrs.fileKey().hashCode());
        stat.setGeneration(attrs.lastModifiedTime().toMillis());

        return stat;
    }

    private int toUnixMode(PosixFileAttributes attributes) {
        int mode = 0;
        if (attributes.isDirectory()) {
            mode |= Stat.S_IFDIR;
        } else if (attributes.isRegularFile()) {
            mode |= Stat.S_IFREG;
        } else if (attributes.isSymbolicLink()) {
            mode |= Stat.S_IFLNK;
        } else {
            mode |= Stat.S_IFSOCK;
        }

        for(PosixFilePermission perm: attributes.permissions()) {
            switch(perm) {
                case GROUP_EXECUTE:  mode |= UnixPermission.S_IXGRP;  break;
                case GROUP_READ:     mode |= UnixPermission.S_IRGRP;  break;
                case GROUP_WRITE:    mode |= UnixPermission.S_IWGRP;  break;
                case OTHERS_EXECUTE: mode |= UnixPermission.S_IXOTH;  break;
                case OTHERS_READ:    mode |= UnixPermission.S_IROTH;  break;
                case OTHERS_WRITE:   mode |= UnixPermission.S_IWOTH;  break;
                case OWNER_EXECUTE:  mode |= UnixPermission.S_IXUSR;  break;
                case OWNER_READ:     mode |= UnixPermission.S_IRUSR;  break;
                case OWNER_WRITE:    mode |= UnixPermission.S_IWUSR;  break;
            }
        }
        return mode;
    }

    @Override
    public int access(Inode inode, int mode) throws IOException {
        return mode;
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        Path path = resolve(inode);
        return statPath(path);
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        // NOP
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        return new nfsace4[0];
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        // NOP
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        return false;
    }

    @Override
    public AclCheckable getAclCheckable() {
        return AclCheckable.UNDEFINED_ALL;
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return _idMapper;
    }
}
