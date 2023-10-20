"""
The FileTransfer client can be used to copy files to the
data locations for file-bound datasets in Metacatalog.
Depending on the location of the client, the FileTransfer class
will either use the OS system to copy files or use the paramiko
SSHClient to copy files to/from a remote server.

"""
from typing import Optional
from paramiko import SSHClient


class FileTransfer:
    def __init__(self, host: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        self.host = host
        self.username = username
        self.password = password
    