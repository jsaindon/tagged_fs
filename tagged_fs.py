#!/usr/bin/env python

import logging
import sys
import errno
import os
import json
import shutil
import stat
import tag_ops
import utils

from fuse import FUSE, Operations, LoggingMixIn, FuseOSError


class TaggedFS(LoggingMixIn, Operations):
    '''
    A tagged filesystem.
    '''

    #QUERY_DELIM            = "$" # Separates tag queries from filenames (i.e. /<tag>/$/<filename>)

    # Metadata constants
    METADATA_FNAME         = "config.json"
    METADATA_TAGS_FOLDER   = "tags_folder"
    METADATA_FILE_FOLDER   = "file_folder"
    METADATA_ACTION_FOLDER = "action_folder"
    METADATA_INODE_COUNTER = "inode_counter"

    # Default values
    DEFAULT_TAGS_FOLDER    = "tags"
    DEFAULT_FILE_FOLDER    = "files"
    DEFAULT_ACTION_FOLDER  = "action"


    def __init__(self, path='.'):
        self.root = path

        # Check for metadata file
        # If file exists, load from it - otherwise, initialize values and create it
        # Should contain: folder names + inode counter
        if not os.path.isfile(TaggedFS.METADATA_FNAME):

            self.tags_folder   = TaggedFS.DEFAULT_TAGS_FOLDER
            self.file_folder   = TaggedFS.DEFAULT_FILE_FOLDER
            self.action_folder = TaggedFS.DEFAULT_ACTION_FOLDER
            self.inode_counter = 0
            
            self.initFilesystem()
        else:
            self.loadMetadataFile(TaggedFS.METADATA_FNAME)

    def chmod(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.ENOSYS)

    def create(self, path, mode):
        path_obj = Path(raw_path)
        action   = path_obj.get_action()
        query    = path_obj.get_query()
        filename = path_obj.get_filename()

        # Ensure that action = /action, query != None, and file != None
        if action != self.action_folder or not query or not filename:
            raise FuseOSError(errno.ENOENT)

        # Ensure that query only contains alphanumeric chars and '+' (otherwise errno bad path)
        for c in query:
            if not c.isalnum() and not (c == "+"):
                raise FuseOSError(errno.EBADF)

        # Strip tags from query component
        tags = query.split('+')


        # Assign inode for file (and increment inode!)
        inode = self.inode_counter
        self.inode_counter += 1

        # Create file
        self.addFile(inode, filename, mode)

        # Create tags if they don't exist and add inode dir entry to each tag
        for tag in tags:
            tag_path = os.path.join(self.root, self.tags_folder, tag)
            if not os.path.isdir(tag_path):
                self.addTag(tag)
            os.makedirs(os.path.join(tag_path, str(inode)))

        # Flush changes to metadata file
        self.saveMetadataFile()

    def destroy(self, path):
        # Store the filesystem state
        self.saveMetadataFile()

    def getattr(self, rel_path, fh=None):
        path = utils.Path(rel_path, self.root)

        # special case for root
        if path.is_root(self.root):
            st = os.lstat(path.get_path())
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
                'st_mode', 'st_mtime', 'st_size', 'st_uid'))

        # Determine what to do from the folder action
        action = path.get_action()

        # Treat tag & file folders normally
        if action == self.tags_folder or action == self.file_folder:
            st = os.lstat(path.get_path())
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
                'st_mode', 'st_mtime', 'st_size', 'st_uid'))

        if action != self.action_folder:
            raise FuseOSError(errno.ENOENT) # not a valid directory in our filesystem

        # Check if file 
        components = path.get_components()
        if len(components) > 2:

            # Must be a file, so attempt search
            result = self.getFilepath(path)

            if not result:
                # Couldn't find the file
                raise FuseOSError(errno.ENOENT)

            # We've found the file path!
            fpath, _ = result
            st = os.lstat(fpath.get_path())
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
                'st_mode', 'st_mtime', 'st_size', 'st_uid'))

        # Otherwise, must be a query string
        # Since a query path refers to a virtual (non-existent) directory,
        # we return default directory attributes
        return dict([('st_atime', 0), ('st_gid', 0), ('st_mode', stat.S_IFDIR), 
            ('st_mtime', 0), ('st_size', 0), ('st_uid', 0)])

    getxattr = None

    def mkdir(self, rel_path, mode):
        path = utils.Path(rel_path, self.root)
        components = path.get_components()

        # Ensure that action is to create tags
        action = path.get_action()

        if action != self.tags_folder:
            raise FuseOSError(errno.EPERM)

        # Create each tag in path that doesn't exist already
        tags = components[1:]

        if len(tags) <= 0:
            raise FuseOSError(errno.EINVAL)

        for tag in tags:
            self.addTag(tag)
        return

    def read(self, rel_path, size, offset, fh):
        path = utils.Path(rel_path, self.root)

        # Must be a file, so attempt search
        result = self.getFilepath(path)
        if not result:
            # Couldn't find the file
            raise FuseOSError(errno.ENOENT)

        # We've found the file path!
        fpath, inode = result
        fh = open(fpath, "rb")

        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def readdir(self, rel_path, fh):
        path = utils.Path(rel_path, self.root)
        action = path.get_action()

        # For tags or file folders, standard behavior
        if action == self.tags_folder or action == self.file_folder:
            return os.listdir(path.get_path())

        if action != self.action_folder:
            raise FuseOSError(errno.ENOENT) # not a valid directory in our filesystem

        # Perform tag query, return corresponding filenames
        query = path.get_query()
        if not query:
            raise FuseOSError(errno.EINVAL)

        tag_folder = os.path.join(self.root, self.tags_folder)
        inodes = tag_ops.get_query_inodes(query, tag_folder)
        filepaths = self.getInodeFilepaths(inodes)        

        return [fpath.get_filename() for fpath, _ in filepaths]

    def readlink(self, path):
        raise FuseOSError(errno.ENOSYS)

    def rename(self, old, new):
        path_obj = Path(new)
        action   = path_obj.get_action()
        query    = path_obj.get_query()
        filename = path_obj.get_filename()

        # Ensure that new tags path component only contains alphanumeric chars and '+' (otherwise errno bad path)
        for c in query:
            if not c.isalnum() and not (c == "+"):
                raise FuseOSError(errno.EBADF)

        # Parse all tags from 'new', w/ format tag1+...+tagn (ensure at least 1+)
        new_tags = c.split("+")

        # Find existing file (inode + filepath)
        fpath, inode = self.getFilepath(old)

        # Remove inode from all tags containing it
        for tag in os.listdir(os.path.join(self.root, self.tags_folder):
            inode_path = os.path.join(self.root, self.tags_folder, tag, str(inode))
            if os.path.isdir(inode_path):
                os.rmdir(inode_path)

        # Create new tags if they don't exist and add inode dir entry to them
        for tag in new_tags:
            tag_path = os.path.join(self.root, self.tags_folder, tag)
            if not os.path.isdir(tag_path):
                self.addTag(tag)
            os.makedirs(os.path.join(tag_path, str(inode)))

    def rmdir(self, rel_path):
        path = utils.Path(rel_path, self.root)

        # Ensure that action is to remove a tag
        action = path.get_action()

        if action != self.tags_folder:
            raise FuseOSError(errno.EPERM)

        # Remove each tag in path
        tags = path.get_components()[1:]

        if len(tags) <= 0:
            raise FuseOSError(errno.EINVAL)

        for tag in tags:
            self.removeTag(tag)
        return

    def symlink(self, target, source):
        raise FuseOSError(errno.ENOSYS)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.ENOSYS)

    def unlink(self, path):
        # Find file (inode + filepath)
        fpath, inode = self.getFilepath(path)

        # Remove inode from all tags containing it
        for tag in os.listdir(os.path.join(self.root, self.tags_folder):
            inode_path = os.path.join(self.root, self.tags_folder, tag, str(inode))
            if os.path.isdir(inode_path):
                os.rmdir(inode_path)

        # Remove file
        self.removeFile(inode, fpath.get_filename())

    def utimens(self, path, times=None):
        raise FuseOSError(errno.ENOSYS)

    def write(self, rel_path, data, offset, fh):
        path = utils.Path(rel_path, self.root)

        # Must be a file, so attempt search
        result = self.getFilepath(path)
        if not result:
            # Couldn't find the file
            raise FuseOSError(errno.ENOENT)

        # We've found the file path!
        fpath, inode = result
        fh = open(fpath, "r+b")

        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, data)

    ####################
    # Helper functions #
    ####################

    def addTag(self, tag):
        tag_path = os.path.join(self.root, self.tags_folder, tag)

        if os.path.isdir(tag_path):
            raise FuseOSError(errno.EEXIST)

        os.makedirs(tag_path)

    def removeTag(self, tag):
        # TODO: EDIT TO HANDLE INODES STORED IN TAG DIRECTORY
        tag_path = os.path.join(self.root, self.tags_folder, tag)

        if not os.path.isdir(tag_path):
            raise FuseOSError(errno.ENOTDIR)

        shutil.rmtree(tag_path)

    def addFile(self, inode, fname, mode):
        fpath = utils.Path(os.path.join(self.file_folder, "/".join([char for char in str(inode)])), self.root)
        fpath = utils.Path.join_paths(fpath, utils.Path(fname))
        
        # Create file
        os.open(fpath.get_path(), os.O_WRONLY | os.O_CREAT, mode)
        return

    def removeFile(self, inode, fname):
        fpath = utils.Path(os.path.join(self.file_folder, "/".join([char for char in str(inode)])), self.root)
        fpath = utils.Path.join_paths(fpath, utils.Path(fname))
        
        # Remove file if it exists
        os.remove(fpath)
        return

    def getInodeFilepath(self, inode):
        filefolder = utils.Path(os.path.join(self.file_folder, "/".join([char for char in str(inode)])), self.root)
        files = [fname for fname in os.listdir(filefolder.get_path()) if os.path.isfile(os.path.join(filefolder.get_path(), fname))]

        if len(files) == 0:
            return None

        return utils.Path.join_paths(filefolder, utils.Path(fname))

    def getInodeFilepaths(self, inodes):
        filepaths = []
        for inode in inodes:
            fpath = self.getInodeFilepath(inode)

            if fpath:
                filepaths.append((fpath, inode))

        return filepaths

    def getFilepath(self, raw_path):
        path = utils.Path(raw_path)
        query = path.get_query()
        fname = path.get_filename()
        if not query or not fname:
            raise FuseOSError(errno.EINVAL)

        tag_folder = os.path.join(self.root, self.tags_folder)
        inodes = tag_ops.get_query_inodes(query, tag_folder)
        fpaths = self.getInodeFilepaths(inodes)

        for fpath, inode in fpaths:
            if fpath.get_filename() == fname:
                return fpath, inode
        return None

    ##############################
    # Filesystem State Functions #
    ##############################

    def initFilesystem(self):
        # Delete any existing folders, since we're
        # starting a fresh filesystem state
        if os.path.exists(os.path.join(self.root, self.tags_folder)):
            shutil.rmtree(os.path.join(self.root, self.tags_folder))
        if os.path.exists(os.path.join(self.root, self.file_folder)):
            shutil.rmtree(os.path.join(self.root, self.file_folder))
        if os.path.exists(os.path.join(self.root, self.action_folder)):
            shutil.rmtree(os.path.join(self.root, self.action_folder))

        # Create necessary folders
        os.makedirs(os.path.join(self.root, self.tags_folder))
        os.makedirs(os.path.join(self.root, self.file_folder))
        os.makedirs(os.path.join(self.root, self.action_folder))

        # Store the filesystem state
        self.saveMetadataFile()

    def saveMetadataFile(self):
        metadata = {
            TaggedFS.METADATA_TAGS_FOLDER   : self.tags_folder,
            TaggedFS.METADATA_FILE_FOLDER   : self.file_folder,
            TaggedFS.METADATA_ACTION_FOLDER : self.action_folder,
            TaggedFS.METADATA_INODE_COUNTER : self.inode_counter,
        }
        json.dump(metadata, open(os.path.join(self.root, TaggedFS.METADATA_FNAME), "w"))

    def loadMetadataFile(self, fname):
        metadata = json.load(open(os.path.join(self.root, TaggedFS.METADATA_FNAME), "r"))
        self.tags_folder   = metadata[TaggedFS.METADATA_TAGS_FOLDER]
        self.file_folder   = metadata[TaggedFS.METADATA_FILE_FOLDER]
        self.action_folder = metadata[TaggedFS.METADATA_ACTION_FOLDER]
        self.inode_counter = metadata[TaggedFS.METADATA_INODE_COUNTER]

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: python %s <mountpoint>' % sys.argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    #mountpoint = os.path.join(os.getcwd(), sys.argv[1])
    fuse = FUSE(TaggedFS(os.getcwd()), sys.argv[1], foreground=True, nothreads=True)