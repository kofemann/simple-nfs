package org.dcache.simplenfs;

import com.google.common.primitives.Longs;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.dcache.nfs.FsExport;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NotEmptyException;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
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

    static public final int S_IRUSR = 00400; // owner has read permission
    static public final int S_IWUSR = 00200; // owner has write permission
    static public final int S_IXUSR = 00100; // owner has execute permission
    static public final int S_IRGRP = 00040; // group has read permission
    static public final int S_IWGRP = 00020; // group has write permission
    static public final int S_IXGRP = 00010; // group has execute permission
    static public final int S_IROTH = 00004; // others have read permission
    static public final int S_IWOTH = 00002; // others have write permission
    static public final int S_IXOTH = 00001; // others have execute

    private final Path _root;
    private final NonBlockingHashMapLong<Path> inodeToPath = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMap<Path, Long> pathToInode = new NonBlockingHashMap<>();
    private final AtomicLong fileId = new AtomicLong(1); //numbering starts at 1
    private final NfsIdMapping _idMapper = new SimpleIdMap();

    private Inode toFh(long inodeNumber) {
        return Inode.forFile(Longs.toByteArray(inodeNumber));
    }

    private long getInodeNumber(Inode inode) {
        return Longs.fromByteArray(inode.getFileId());
    }

    private Path resolveInode(long inodeNumber) throws NoEntException {
        Path path = inodeToPath.get(inodeNumber);
        if (path == null) {
            throw new NoEntException("inode #" + inodeNumber);
        }
        return path;
    }

    private long resolvePath(Path path) throws NoEntException {
        Long inodeNumber = pathToInode.get(path);
        if (inodeNumber == null) {
            throw new NoEntException("path " + path);
        }
        return inodeNumber;
    }

    private void map(long inodeNumber, Path path) {
        if (inodeToPath.putIfAbsent(inodeNumber, path) != null) {
            throw new IllegalStateException();
        }
        Long otherInodeNumber = pathToInode.putIfAbsent(path, inodeNumber);
        if (otherInodeNumber != null) {
            //try rollback
            if (inodeToPath.remove(inodeNumber) != path) {
                throw new IllegalStateException("cant map, rollback failed");
            }
            throw new IllegalStateException("path ");
        }
    }

    private void unmap(long inodeNumber, Path path) {
        Path removedPath = inodeToPath.remove(inodeNumber);
        if (!path.equals(removedPath)) {
            throw new IllegalStateException();
        }
        if (pathToInode.remove(path) != inodeNumber) {
            throw new IllegalStateException();
        }
    }

    private void remap(long inodeNumber, Path oldPath, Path newPath) {
        //TODO - attempt rollback?
        unmap(inodeNumber, oldPath);
        map(inodeNumber, newPath);
    }

    public LocalFileSystem(Path root, Iterable<FsExport> exportIterable) throws IOException {
        _root = root;
        assert (Files.exists(_root));
        for (FsExport export : exportIterable) {
            Path exportRootPath = root.resolve(export.getPath());
            if (!Files.exists(exportRootPath)) {
                Files.createDirectories(exportRootPath);
            }
        }
        //map existing structure (if any)
        map(fileId.getAndIncrement(), _root); //so root is always inode #1
        Files.walkFileTree(_root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                FileVisitResult superRes = super.preVisitDirectory(dir, attrs);
                if (superRes != FileVisitResult.CONTINUE) {
                    return superRes;
                }
                if (dir.equals(_root)) {
                    return FileVisitResult.CONTINUE;
                }
                map(fileId.getAndIncrement(), dir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                FileVisitResult superRes = super.visitFile(file, attrs);
                if (superRes != FileVisitResult.CONTINUE) {
                    return superRes;
                }
                map(fileId.getAndIncrement(), file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public Inode create(Inode parent, Type type, String path, Subject subject, int mode) throws IOException {
        long parentInodeNumber = getInodeNumber(parent);
        Path parentPath = resolveInode(parentInodeNumber);
        Path newPath = parentPath.resolve(path);
        try {
            Files.createFile(newPath);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        long newInodeNumber = fileId.getAndIncrement();
        map(newInodeNumber, newPath);
        return toFh(newInodeNumber);
    }

    @Override
    public FsStat getFsStat() throws IOException {
        FileStore store = Files.getFileStore(_root);
        long total = store.getTotalSpace();
        long free = store.getUsableSpace();
        return new FsStat(total, Long.MAX_VALUE, total-free, pathToInode.size());
    }

    @Override
    public Inode getRootInode() throws IOException {
        return toFh(1); //always #1 (see constructor)
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        //TODO - several issues
        //1. we might not deal with "." and ".." properly
        //2. we might accidentally allow composite paths here ("/dome/dir/down")
        //3. we dont actually check that the parent exists
        long parentInodeNumber = getInodeNumber(parent);
        Path parentPath = resolveInode(parentInodeNumber);
        Path child = parentPath.resolve(path);
        long childInodeNumber = resolvePath(child);
        return toFh(childInodeNumber);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        final List<DirectoryEntry> list = new ArrayList<>();
        Files.newDirectoryStream(path).forEach(p -> {
            long inodeNumber1;
            try {
                inodeNumber1 = resolvePath(p);
                list.add(new DirectoryEntry(p.getFileName().toString(), toFh(inodeNumber1), statPath(p, inodeNumber1)));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        long parentInodeNumber = getInodeNumber(parent);
        Path parentPath = resolveInode(parentInodeNumber);
        Path newPath = parentPath.resolve(path);
        try {
            Files.createDirectory(newPath);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        long newInodeNumber = fileId.getAndIncrement();
        map(newInodeNumber, newPath);
        return toFh(newInodeNumber);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        //TODO - several issues
        //1. we might not deal with "." and ".." properly
        //2. we might accidentally allow composite paths here ("/dome/dir/down")
        //3. we return true (changed) even though in theory a file might be renamed to itself?
        long currentParentInodeNumber = getInodeNumber(src);
        Path currentParentPath = resolveInode(currentParentInodeNumber);
        long destParentInodeNumber = getInodeNumber(dest);
        Path destPath = resolveInode(destParentInodeNumber);
        Path currentPath = currentParentPath.resolve(oldName);
        long targetInodeNumber = resolvePath(currentPath);
        Path newPath = destPath.resolve(newName);
        try {
            Files.move(currentPath, newPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        remap(targetInodeNumber, currentPath, newPath);
        return true;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        if (inodeNumber == 1) {
            throw new NoEntException("no parent"); //its the root
        }
        Path path = resolveInode(inodeNumber);
        Path parentPath = path.getParent();
        long parentInodeNumber = resolvePath(parentPath);
        return toFh(parentInodeNumber);
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        ByteBuffer destBuffer = ByteBuffer.wrap(data, 0, count);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.read(destBuffer, offset);
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        return Files.readSymbolicLink(path).toString();
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        long parentInodeNumber = getInodeNumber(parent);
        Path parentPath = resolveInode(parentInodeNumber);
        Path targetPath = parentPath.resolve(path);
        long targetInodeNumber = resolvePath(targetPath);
        try {
            Files.delete(targetPath);
        } catch (DirectoryNotEmptyException e) {
            throw new NotEmptyException("dir " + targetPath + " is note empty");
        }
        unmap(targetInodeNumber, targetPath);
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        ByteBuffer srcBuffer = ByteBuffer.wrap(data, 0, count);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            int bytesWritten = channel.write(srcBuffer, offset);
            return new WriteResult(StabilityLevel.FILE_SYNC, bytesWritten);
        }
    }

    @Override
    public void commit(Inode inode, long l, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private Stat statPath(Path p, long inodeNumber) throws IOException {
        PosixFileAttributes attrs = Files.getFileAttributeView(p, PosixFileAttributeView.class).readAttributes();

        Stat stat = new Stat();

        stat.setATime(attrs.lastAccessTime().toMillis());
        stat.setCTime(attrs.creationTime().toMillis());
        stat.setMTime(attrs.lastModifiedTime().toMillis());

        // FIXME
        stat.setGid(0);
        stat.setUid(0);

        stat.setDev(17);
        stat.setIno((int) inodeNumber);
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
                case GROUP_EXECUTE:  mode |= S_IXGRP;  break;
                case GROUP_READ:     mode |= S_IRGRP;  break;
                case GROUP_WRITE:    mode |= S_IWGRP;  break;
                case OTHERS_EXECUTE: mode |= S_IXOTH;  break;
                case OTHERS_READ:    mode |= S_IROTH;  break;
                case OTHERS_WRITE:   mode |= S_IWOTH;  break;
                case OWNER_EXECUTE:  mode |= S_IXUSR;  break;
                case OWNER_READ:     mode |= S_IRUSR;  break;
                case OWNER_WRITE:    mode |= S_IWUSR;  break;
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
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        return statPath(path, inodeNumber);
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        long inodeNumber = getInodeNumber(inode);
        Path path = resolveInode(inodeNumber);
        BasicFileAttributeView attributeView = Files.getFileAttributeView(path, BasicFileAttributeView.class);
        //TODO - implement fully
        FileTime aTime = null;
        FileTime mTime = null;
        if (stat.isDefined(Stat.StatAttribute.OWNER)) {
            throw new UnsupportedOperationException("set oid unsupported");
        }
        if (stat.isDefined(Stat.StatAttribute.GROUP)) {
            throw new UnsupportedOperationException("set gid unsupported");
        }
        if (stat.isDefined(Stat.StatAttribute.SIZE)) {
            //little known fact - truncate() returns the original channel
            //noinspection EmptyTryBlock
            try (FileChannel ignored = FileChannel.open(path, StandardOpenOption.WRITE).truncate(stat.getSize())) {}
        }
        if (stat.isDefined(Stat.StatAttribute.ATIME)) {
            throw new UnsupportedOperationException("set atime unsupported");
        }
        if (stat.isDefined(Stat.StatAttribute.MTIME)) {
            mTime = FileTime.fromMillis(stat.getMTime());
        }
        if (aTime != null || mTime != null) {
            attributeView.setTimes(mTime, aTime, null);
        }
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
