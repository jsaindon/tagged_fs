#!/usr/bin/env python

import logging
import sys
import errno
import os

from fuse import FUSE, Operations, LoggingMixIn, FuseOSError


class TaggedFS(LoggingMixIn, Operations):
    '''
    A tagged filesystem.
    '''

    def __init__(self, path='.'):
        self.root = path

    def chmod(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.ENOSYS)

    def create(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    def destroy(self, path):
        raise FuseOSError(errno.ENOSYS)

    def getattr(self, path, fh=None):
        raise FuseOSError(errno.ENOSYS)

    def mkdir(self, path, mode):
        return self.sftp.mkdir(path, mode)

    def read(self, path, size, offset, fh):
        raise FuseOSError(errno.ENOSYS)

    def readdir(self, path, fh):
        raise FuseOSError(errno.ENOSYS)

    def readlink(self, path):
        raise FuseOSError(errno.ENOSYS)

    def rename(self, old, new):
        raise FuseOSError(errno.ENOSYS)

    def rmdir(self, path):
        raise FuseOSError(errno.ENOSYS)

    def symlink(self, target, source):
        raise FuseOSError(errno.ENOSYS)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.ENOSYS)

    def unlink(self, path):
        raise FuseOSError(errno.ENOSYS)

    def utimens(self, path, times=None):
        raise FuseOSError(errno.ENOSYS)

    def write(self, path, data, offset, fh):
        raise FuseOSError(errno.ENOSYS)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: python %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    fuse = FUSE(TaggedFS(os.getcwd()), sys.argv[1], foreground=True, nothreads=True)