"""Standalone FTP/explicit FTPS listener rooted at an existing NAS folder."""
import argparse
import os
import logging
from uuid import uuid4
from pathlib import Path

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.filesystems import AbstractedFS
from pyftpdlib.handlers import FTPHandler, TLS_FTPHandler, DTPHandler, TLS_DTPHandler
from pyftpdlib.servers import FTPServer

from .protocols import ConnectorError, MAX_BYTES, secret


class ExclusiveFilesystem(AbstractedFS):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.staged={}

    def open(self,filename,mode):
        if mode!="wb":
            return super().open(filename,mode)
        target=Path(filename)
        if os.path.lexists(target):
            raise FileExistsError("Destination exists")
        staging=Path(self.root)/".relay-incoming"
        staging.mkdir(exist_ok=True)
        if not staging.resolve().is_relative_to(Path(self.root).resolve()):
            raise OSError("Staging folder escaped root")
        temporary=staging/uuid4().hex
        self.staged[str(temporary)]=str(target)
        return super().open(str(temporary),"xb")

    def listdir(self,path):
        return [name for name in super().listdir(path) if name!=".relay-incoming"]

    def validpath(self,path):
        if ".relay-incoming" in Path(path).parts:
            return False
        return super().validpath(path)

    def complete(self,temporary,finished):
        target=self.staged.pop(temporary,None)
        if not target:
            return
        try:
            if finished:
                if not Path(target).resolve().is_relative_to(Path(self.root).resolve()):
                    raise OSError("Destination escaped root")
                # Both files are on the same root filesystem. An exclusive hard link
                # publishes only complete bytes and never overwrites a concurrent file.
                os.link(temporary,target)
        finally:
            Path(temporary).unlink(missing_ok=True)


class AtomicTransfer:
    def recv(self,size):
        data=super().recv(size)
        if self.receive and self.tot_bytes_received+len(data)>MAX_BYTES:
            raise OSError("FTP document exceeds the 10 MiB limit")
        return data

    def close(self):
        if not self._closed and self.receive and self.file_obj is not None:
            temporary=self.file_obj.name
            if not self.file_obj.closed:
                self.file_obj.close()
            try:
                self.cmd_channel.fs.complete(temporary,self.transfer_finished)
            except OSError:
                self.transfer_finished=False
                self._resp=("426 Document publication failed; verify NAS hard-link support and destination.",logging.getLogger("relay.ftp").warning)
        super().close()


class NoOverwrite:
    abstracted_fs=ExclusiveFilesystem
    def ftp_STOR(self,file,mode="w"):
        if mode!="w" or os.path.lexists(file):
            self.respond("550 Existing files cannot be overwritten or appended.")
            return
        return super().ftp_STOR(file,mode)
    def ftp_APPE(self,file):
        self.respond("550 Append is disabled.")
    def ftp_STOU(self,line):
        self.respond("502 STOU is not supported; send an explicit unique filename.")


def serve(root,host,port,username,password,certificate=None,key=None):
    root=Path(root).resolve(strict=True)
    if not root.is_dir():
        raise ConnectorError("FTP listener root must be an existing directory")
    if not username or not password:
        raise ConnectorError("FTP listener requires username and password")
    authorizer=DummyAuthorizer()
    # LIST, READ, WRITE, MKDIR only. No delete, rename, overwrite or shell.
    authorizer.add_user(username,password,str(root),perm="elrw m".replace(" ",""))
    base=TLS_FTPHandler if certificate else FTPHandler
    handler=type("RelayFTPHandler",(NoOverwrite,base),{})
    handler.dtp_handler=type("RelayDTPHandler",(AtomicTransfer,TLS_DTPHandler if certificate else DTPHandler),{})
    handler.authorizer=authorizer
    handler.banner="Relay file transfer"
    if certificate:
        handler.certfile=str(certificate);handler.keyfile=str(key or certificate)
        handler.tls_control_required=True;handler.tls_data_required=True
    server=FTPServer((host,port),handler)
    server.max_cons=50;server.max_cons_per_ip=10
    try:
        server.serve_forever(timeout=0.5)
    finally:
        server.close_all()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=2121)
    parser.add_argument("--username-env",default="FTP_USERNAME")
    parser.add_argument("--password-env",default="FTP_PASSWORD")
    parser.add_argument("--certificate",type=Path,help="PEM certificate for explicit FTPS")
    parser.add_argument("--key",type=Path,help="PEM key for explicit FTPS; defaults to certificate file")
    args=parser.parse_args()
    serve(args.root,args.host,args.port,secret(args.username_env,True),secret(args.password_env,True),args.certificate,args.key)


if __name__=="__main__":
    main()
