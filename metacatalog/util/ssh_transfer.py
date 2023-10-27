"""
The FileTransfer client can be used to copy files to the
data locations for file-bound datasets in Metacatalog.
Depending on the location of the client, the FileTransfer class
will either use the OS system to copy files or use the paramiko
SSHClient to copy files to/from a remote server.

"""
from typing import Optional
import os
import shutil
import glob

from paramiko import SSHClient
from dotenv import load_dotenv
from tqdm import tqdm

# load an environment file
load_dotenv()


class FileTransfer:  # pragma: no cover
    def __init__(self, host: str = 'localhost', username: Optional[str] = None, password: Optional[str] = None, quiet: bool = False):
        self.host = host
        self.username = username
        self.password = password
        self.quiet = quiet

        # figure out if we need to use SSH
        self.is_local = 'localhost' in self.host

    def get(self, source: str, target: str) -> None:
        """Copy a file from source to target."""
        if self.is_local:
            self._local_copy(source, target)
        else:
            self._ssh_get(source, target)

    def put(self, source: str, target: str) -> None:
        """Copy a file from source to target."""
        if self.is_local:
            self._local_copy(source, target)
        else:
            self._ssh_put(source, target)

    def _local_copy(self, source: str, target: str) -> None:
        if os.path.isfile(source):            
            # copy the file
            return shutil.copy(source, target)
        
        # we are in a directory
        
        # check  if we are recursive, then create needed directories
        os.makedirs(target, exist_ok=True)

        # get all contents of the source
        flist = glob.glob(os.path.join(source, '*'))

        # check verbosity
        if self.quiet:
            _iterator = flist
        else:
            _iterator = tqdm(flist)
        
        # go for each file
        for fname in _iterator:
            # get the relative path for fname to the source
            rel_path = os.path.relpath(fname, source)
            self._local_copy(fname, os.path.join(target, rel_path))
