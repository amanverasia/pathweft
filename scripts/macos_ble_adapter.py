"""Own one unmodified, already Bluetooth-authorized Mac central app for a proof."""
import json
from pathlib import Path
import signal
import shlex
import subprocess
import time

from lan_demo import require


class MacAdapter:
    def __init__(self,node,app,local,index):
        self.node=node;self.app=Path(app).resolve(strict=True)
        self.binary=self.app/'Contents/MacOS/pathweft-ble'
        require(self.binary.is_file(),'Mac BLE executable missing')
        self.local_address=local;self.log=f'ble-{index}.log'
        self.path=Path(node.root)/self.log;self.pid=None
        require(not self.processes(),'Mac BLE app already running; leave it alone')
        self.path.touch(exist_ok=False)
    def processes(self):
        result=subprocess.run(['ps','-ww','-axo','pid=,command='],capture_output=True,text=True,check=True,timeout=10)
        matches={}
        for line in result.stdout.splitlines():
            parts=line.strip().split(None,1)
            if len(parts)!=2:continue
            pid,command=parts
            if command==str(self.binary) or command.startswith(str(self.binary)+' '):matches[int(pid)]=command
        return matches
    def start(self):
        subprocess.run(['open','-n',str(self.app),'--stdout',str(self.path),'--stderr',str(self.path),'--args','connect',
            '--local',self.local_address,'--node',self.node.status['listen']],check=True,timeout=15)
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            matches={pid:command for pid,command in self.processes().items() if self.owned(command)}
            if matches:
                require(len(matches)==1,'more than one matching Mac app appeared')
                self.pid=next(iter(matches));return
            time.sleep(.1)
        raise TimeoutError('Mac app did not start')
    def owned(self,command):
        try:
            args=shlex.split(command)
            return args[0]==str(self.binary) and all(
                args[args.index(flag)+1]==value
                for flag,value in [('--local',self.local_address),('--node',self.node.status['listen'])])
        except (ValueError,IndexError):return False
    def rows(self):
        result=[]
        for line in self.path.read_text().splitlines():
            try:result.append(json.loads(line))
            except ValueError:pass
        return result
    def count(self,event):return sum(row.get('event')==event for row in self.rows())
    def local(self):return next((row['details']['local'] for row in self.rows() if row.get('event')=='link_up'),None)
    def stop(self):
        if self.pid is None:return
        command=self.processes().get(self.pid)
        if command is None:return
        require(self.owned(command),'Mac PID no longer matches owned arguments')
        try:signal_pid(self.pid,signal.SIGINT)
        except ProcessLookupError:return
        deadline=time.monotonic()+5
        while self.pid in self.processes() and time.monotonic()<deadline:time.sleep(.1)
        command=self.processes().get(self.pid)
        if command is not None:
            require(self.owned(command),'Mac PID changed ownership while stopping')
            signal_pid(self.pid,signal.SIGKILL)
            deadline=time.monotonic()+3
            while self.pid in self.processes() and time.monotonic()<deadline:time.sleep(.1)
        require(self.pid not in self.processes(),'owned Mac app did not stop')


def signal_pid(pid,kind):
    import os
    os.kill(pid,kind)
