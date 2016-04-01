#!/usr/bin/env python

import logging
import sys
import errno
import os
import json
import shutil
import stat

from fuse import FUSE, Operations, LoggingMixIn, FuseOSError


class TaggedFS(LoggingMixIn, Operations):
    '''
    A tagged filesystem.
    '''

    QUERY_DELIM            = "$" # Separates tag queries from filenames (i.e. /<tag>/$/<filename>)

    # Metadata constants
    METADATA_FNAME         = "config.json"
    METADATA_TAGS_FOLDER   = "tags_folder"
    METADATA_FILE_FOLDER   = "file_folder"
    METADATA_QUERY_FOLDER  = "query_folder"
    METADATA_INODE_COUNTER = "inode_counter"

    # Default values
    DEFAULT_TAGS_FOLDER    = "tags"
    DEFAULT_FILE_FOLDER    = "files"
    DEFAULT_QUERY_FOLDER   = "query"


    def __init__(self, path='.'):
        self.root = path

        # Check for metadata file
        # If file exists, load from it - otherwise, initialize values and create it
        # Should contain: folder names + inode counter
        if not os.path.isfile(TaggedFS.METADATA_FNAME):

            self.tags_folder   = TaggedFS.DEFAULT_TAGS_FOLDER
            self.file_folder   = TaggedFS.DEFAULT_FILE_FOLDER
            self.query_folder = TaggedFS.DEFAULT_QUERY_FOLDER
            self.inode_counter = 0
            
            self.initFilesystem()
        else:
            self.loadMetadataFile(TaggedFS.METADATA_FNAME)

    def chmod(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.ENOSYS)

    def create(self, path, mode):
        raise FuseOSError(errno.ENOSYS)

    def destroy(self, path):
        raise FuseOSError(errno.ENOSYS)

    def getattr(self, path, fh=None):
        # special case for root
        if path == "/":
            st = os.lstat(self.absPath(path))
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
                'st_mode', 'st_mtime', 'st_size', 'st_uid'))


        print("path: " + path)
        split_path = path.split(TaggedFS.QUERY_DELIM)

        # If no query delimiter, or not in tags, query is incomplete
        # hack: we pretend it's a generic directory to get to the rest of the query
        # Special case: creating a tag needs to be able to check for tag existence
        if len(split_path) < 2:
            action = self.getAction(path)
            if action == self.tags_folder:
                st = os.lstat(self.absPath(path))
                return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
                    'st_mode', 'st_mtime', 'st_size', 'st_uid'))

            filepath = self.absPath(path.split('/')[-1])
            print(filepath)
            return dict([('st_atime', 0), ('st_gid', 0), ('st_mode', stat.S_IFDIR), 
                ('st_mtime', 0), ('st_size', 0), ('st_uid', 0)])
        else:
            # Ensure that a correct action is being taken
            action = self.getAction(path)
            if action not in [self.tags_folder, self.file_folder, self.query_folder]:
                raise FuseOSError(errno.EINVAL)

            # Ensure that file is in search results, and get full path
            query_path, fname = split_path

            # If filename is empty, we've hit the delimiter and need to skip to the next call
            if not fname:
                return dict([('st_atime', 0), ('st_gid', 0), ('st_mode', stat.S_IFDIR), 
                    ('st_mtime', 0), ('st_size', 0), ('st_uid', 0)])

            filepaths = self.tagSearch(query_path)
            filepath = self.absPath(os.path.join(self.tags_folder, next(fpath for fpath in filepaths if fpath.split('/')[-1] == fname)))
            print(filepath)
        st = os.lstat(filepath)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_gid',
            'st_mode', 'st_mtime', 'st_size', 'st_uid'))

    getxattr = None

    def mkdir(self, path, mode):
        components = path.lstrip('/').split('/')

        # Ensure that action is to create tags
        action = components[0]

        if action != self.tags_folder:
            raise FuseOSError(errno.EPERM)

        # Create each tag in path that doesn't exist already
        tags = components[1:]

        if len(tags) <= 0:
            raise FuseOSError(errno.EINVAL)

        for tag in tags:
            self.addTag(tag)
        return

    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def readdir(self, path, fh):
        #return ['hello_world'] # placeholder for testing
        query_path, fname = self.splitQueryPath(path)

        action = self.getAction(query_path)

        # For tags folder, show all tags
        if action == self.tags_folder:
            return os.listdir(self.absPath(self.tags_folder))

        # For files folder, <Either error or all files?>

        if len(fname) > 0:
            raise FuseOSError(errno.ENOTDIR)

        return [fpath[-1] for fpath in self.tagSearch(query_path)]

    def readlink(self, path):
        raise FuseOSError(errno.ENOSYS)

    def rename(self, old, new):
        raise FuseOSError(errno.ENOSYS)

    def rmdir(self, path):
        components = path.lstrip('/').split('/')

        # Ensure that action is to remove a tag
        action = components[0]

        if action != self.tags_folder:
            raise FuseOSError(errno.EPERM)

        # Remove each tag in path
        tags = components[1:]

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
        raise FuseOSError(errno.ENOSYS)

    def utimens(self, path, times=None):
        raise FuseOSError(errno.ENOSYS)

    def write(self, path, data, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, data)

    ####################
    # Helper functions #
    ####################

    def getAction(self, path):
        return path.lstrip('/').split('/')[0]

    def absPath(self, path):
        return os.path.join(self.root, path.lstrip('/'))

    def addTag(self, tag):
        tag_path = os.path.join(self.root, self.tags_folder, tag)

        if os.path.isdir(tag_path):
            raise FuseOSError(errno.EEXIST)

        os.makedirs(tag_path)

    def removeTag(self, tag):
        tag_path = self.absPath(os.path.join(self.tags_folder, tag))

        if not os.path.isdir(tag_path):
            raise FuseOSError(errno.ENOTDIR)

        shutil.rmtree(tag_path)

    '''
    Search functions
    '''

    def splitQueryPath(self, path):
        # Return values:
        #   query_path
        #   filename (possibly empty)
        split_path = path.split(TaggedFS.QUERY_DELIM)

        # If no query delimiter, not valid query
        if len(split_path) < 2:
            raise FuseOSError(errno.EINVAL)
        
        query_path, after_delim = split_path
        fname = after_delim.lstrip('/')
        return query_path, fname

    def tagSearch(self, rel_path):
        """
        Searches the filesystem based on the relative path query.
        Returns a list of filepaths that match the query.
        """
        components = rel_path.split('/')[1:] # Ignore empty string from root '/'

        # Action folder required
        if len(components) <= 0:
            raise(FuseOSError(errno.EACCES))

        action = components[0]
        tags = components[1:]

        if len(tags) <= 0:
            return [] # no tags = no results

        # Get relevants files for each tag
        resultSets = [set(self.getFilesForTag(tag)) for tag in tags]

        # Return intersection of tags results
        searchResults = resultSets[0]
        for resultSet in resultSets[1:]:
            searchResults = searchResults & resultSet

        return list(searchResults)

    def getFilesForTag(self, tag):
        print(os.path.join(self.root, self.tags_folder, tag))
        inodes = os.listdir(os.path.join(self.root, self.tags_folder, tag))

        filepaths = []
        for inode in inodes:
            filefolder = os.path.join(self.root, "/".join([char for char in str(inode)]))
            files = [fname for fname in os.listdir(filefolder) if os.path.isfile(os.path.join(filefolder, fname))]

            if len(files) == 0:
                continue

            filepaths.append(os.path.join(filefolder, files[0]))

        return filepaths

    ''' 
    Filesystem state functions
    '''

    def initFilesystem(self):
        # Delete any existing folders, since we're
        # starting a fresh filesystem state
        if os.path.exists(os.path.join(self.root, self.tags_folder)):
            shutil.rmtree(os.path.join(self.root, self.tags_folder))
        if os.path.exists(os.path.join(self.root, self.file_folder)):
            shutil.rmtree(os.path.join(self.root, self.file_folder))
        if os.path.exists(os.path.join(self.root, self.query_folder)):
            shutil.rmtree(os.path.join(self.root, self.query_folder))

        # Create necessary folders
        os.makedirs(os.path.join(self.root, self.tags_folder))
        os.makedirs(os.path.join(self.root, self.file_folder))
        os.makedirs(os.path.join(self.root, self.query_folder))

        # Store the filesystem state
        self.saveMetadataFile()

    def saveMetadataFile(self):
        metadata = {
            TaggedFS.METADATA_TAGS_FOLDER   : self.tags_folder,
            TaggedFS.METADATA_FILE_FOLDER   : self.file_folder,
            TaggedFS.METADATA_QUERY_FOLDER : self.query_folder,
            TaggedFS.METADATA_INODE_COUNTER : self.inode_counter,
        }
        json.dump(metadata, open(os.path.join(self.root, TaggedFS.METADATA_FNAME), "w"))

    def loadMetadataFile(self, fname):
        metadata = json.load(open(os.path.join(self.root, TaggedFS.METADATA_FNAME), "r"))
        self.tags_folder   = metadata[TaggedFS.METADATA_TAGS_FOLDER]
        self.file_folder   = metadata[TaggedFS.METADATA_FILE_FOLDER]
        self.query_folder = metadata[TaggedFS.METADATA_QUERY_FOLDER]
        self.inode_counter = metadata[TaggedFS.METADATA_INODE_COUNTER]

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: python %s <mountpoint>' % sys.argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    #mountpoint = os.path.join(os.getcwd(), sys.argv[1])
    fuse = FUSE(TaggedFS(os.getcwd()), sys.argv[1], foreground=True, nothreads=True)