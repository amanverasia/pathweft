#!/usr/bin/env python3
"""BLE relay proof on two Linux hosts: logical A/B on server host, C on client.

No pairing, Wi-Fi, power or global discoverability changes. Nodes are loopback
only. The report distinguishes the real radio hop from the local A-B hop.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import select
import subprocess
import time

from lan_demo import Node, require
from ble_relay_demo import ADAPTER_HELPER

READ = """import json,sys;from pathlib import Path
r=json.load(sys.stdin);p=Path(r['root']);assert p.parent==Path('/tmp') and p.name.startswith('pw-net-')
name=r['log'];assert name.startswith('ble-') and name.endswith('.log') and '/' not in name
f=p/name;print(f.read_text() if f.exists() else '')
"""
CONTROL = """import json,socket,struct,sys
r=json.load(sys.stdin);s=socket.socket(socket.AF_UNIX);s.settimeout(10);s.connect(r['root']+'/control.sock')
b=json.dumps(r['request']).encode();s.sendall(struct.pack('>I',len(b))+b)
def read(n):
 b=b''
 while len(b)<n:
  v=s.recv(n-len(b))
  if not v:raise RuntimeError('truncated control response')
  b+=v
 return b
n=struct.unpack('>I',read(4))[0];assert 0<n<=32768;print(read(n).decode())
"""


def wait(description, action, seconds=40):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=action()
        if result:return result
        time.sleep(.25)
    raise TimeoutError(description)


def remote(node, helper, payload):
    result=subprocess.run(node.command(helper),input=json.dumps(payload),capture_output=True,text=True,check=True,timeout=20)
    return result.stdout


def connected_channel(rows):
    attempted=connected=None
    for row in rows:
        if row.get('event')=='discovered':attempted=row['details']['psm']
        elif row.get('event')=='link_up':connected=attempted
    return connected


class Adapter:
    def __init__(self, node, command, index):
        self.node,self.log,self.process=node,f'ble-{index}.log',None
        self.process=subprocess.Popen(node.command(ADAPTER_HELPER),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.process.stdin.write(json.dumps({'root':node.root,'command':command,'log':self.log})+'\n');self.process.stdin.flush()
            require(select.select([self.process.stdout],[],[],10)[0],'adapter launcher timed out')
            require('pid' in json.loads(self.process.stdout.readline()),'adapter did not start')
        except BaseException:
            self.stop()
            raise
    def rows(self):
        result=[]
        for line in remote(self.node,READ,{'root':self.node.root,'log':self.log}).splitlines():
            try:result.append(json.loads(line))
            except ValueError:pass
        return result
    def count(self,event):return sum(row.get('event')==event for row in self.rows())
    def local(self):return next((r['details']['local'] for r in self.rows() if r.get('event')=='link_up'),None)
    def stop(self):
        try:
            if self.process.poll() is None:
                if not self.process.stdin.closed:
                    try:self.process.stdin.write('stop\n');self.process.stdin.flush()
                    except (BrokenPipeError,OSError):pass
                    # A quick stop may already be in Python's readline buffer.
                    # EOF wakes the helper's select even in that case.
                    self.process.stdin.close()
                self.process.wait(timeout=10)
        finally:
            # EOF also asks the attached helper to stop its own adapter.
            for stream in [self.process.stdin,self.process.stdout,self.process.stderr]:stream.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--change-channel',action='store_true',help='reserve the old PSM on restart; config needs channel_reserver')
    args=parser.parse_args();config=json.loads(args.config.read_text())
    a,b,c=[Node(spec) for spec in config['nodes']]
    nodes=[a,b,c]
    require([n.name for n in nodes]==list('ABC'),'need nodes A/B/C')
    require(all(n.address=='127.0.0.1' for n in nodes),'all data listeners must be loopback')
    require(a.ssh==b.ssh and a.ssh!=c.ssh,'A/B must share server host; C must use another host')
    require(not args.change_channel or config.get('channel_reserver'),'--change-channel requires channel_reserver on the server host')
    report={'test':'linux-pi-ble-relay','passed':False,'utc':datetime.now(timezone.utc).isoformat(),'checks':[],
        'topology':'A/B share server host; C on second host; A-B loopback, B-C LE L2CAP',
        'internet_isolation':'not measured; WAN remains connected','adapters':[]}
    adapters=[]
    def passed(name,ok=True):
        require(ok,name);report['checks'].append(name);print('PASS '+name,flush=True)
    def port(node):
        helper="import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()"
        result=subprocess.run(node.command(helper),capture_output=True,text=True,check=True,timeout=15)
        return '127.0.0.1:'+result.stdout.strip()
    def start_server(local):
        adapter=Adapter(b,[config['server_ble_binary'],'serve','--local',local,'--node',b.status['listen']],len(adapters))
        adapters.append(adapter);wait('server advertising',lambda:adapter.count('advertising'))
        return adapter
    def route(source,destination,via):
        source.cli('route','--socket',source.root+'/control.sock','--to',destination.identity['id'],'--via',via)
    def toggle(enabled):
        result=json.loads(remote(b,CONTROL,{'root':b.root,'request':{'command':'relay','enabled':enabled}}))
        require(result['ok'],'relay control failed')
    def text(label):
        value=label
        while len((value+' café 🌍').encode())<=512:value+=' café 🌍'
        return value+'.'*(512-len(value.encode()))
    try:
        for node in nodes:node.prepare()
        report['nodes']=[{'name':n.name,**n.info} for n in nodes]
        for node in nodes:node.start(nodes)
        server_port,client_port=port(b),port(c)
        server=start_server(server_port)
        command=[config['client_ble_binary'],'connect','--local',client_port,'--node',c.status['listen']]
        if config.get('server_peer'):command+=['--peer',config['server_peer']]
        client=Adapter(c,command,len(adapters));adapters.append(client)
        wait('radio link on server',server.local,60);wait('radio link on client',client.local,60)
        passed('BLE channel discovered and opened')
        advertised=next(r['details']['psm'] for r in server.rows() if r.get('event')=='advertising')
        discovered=connected_channel(client.rows())
        passed('client learned matching advertised channel without a manual PSM',advertised==discovered)
        for target in [b,c]:route(a,target,b.status['listen'])
        route(b,a,a.status['listen']);route(b,c,server_port)
        for target in [a,b]:route(c,target,client_port)
        outgoing,incoming=text('A via B to C'),text('C via B to A')
        a.send(c,outgoing);c.send(a,incoming)
        passed('512-byte relayed text and authenticated receipts both ways')
        for node,expected in [(a,incoming),(c,outgoing)]:
            rows=[json.loads(line) for line in node.call('log')['log'].splitlines()]
            passed(node.name+' recovered exact UTF-8',any(r.get('event')=='text_received' and r['details'].get('text')==expected for r in rows))
        relay=b.inspect();report['relay_counters']=relay
        passed('relay forwarded both texts and receipts',relay['forwarded']>=4 and relay['delivered_texts']==0)
        passed('relay diagnostics contain no private text',outgoing not in b.call('log')['log'] and incoming not in b.call('log')['log'])
        toggle(False);a.send(c,'transit must fail',succeeds=False);b.send(c,"relay owner's own message")
        passed('relay opt-out blocks transit and preserves owner messaging');toggle(True)
        server.stop();wait('client noticed radio loss',lambda:client.count('link_down'))
        a.send(c,'radio down; must fail',succeeds=False);c.send(a,'return radio down; must fail',succeeds=False)
        passed('stopping the radio adapter prevents delivery in both directions')
        if args.change_channel:
            reservation=Adapter(b,[config['channel_reserver'],str(advertised)],len(adapters))
            adapters.append(reservation)
            wait('old channel reserved',lambda:reservation.count('channel_reserved'))
        before=client.count('link_up');restarted_at=time.monotonic();server=start_server(server_port)
        new_psm=next(r['details']['psm'] for r in server.rows() if r.get('event')=='advertising')
        report['channels']={'initial':advertised,'restarted':new_psm}
        if args.change_channel:passed('restarted server advertised a different channel',new_psm!=advertised)
        wait('server reconnected',server.local,60);wait('client reconnected',lambda:client.count('link_up')>before,60)
        report['reconnect_seconds']=round(time.monotonic()-restarted_at,2)
        passed('client learned the restarted advertised channel',connected_channel(client.rows())==new_psm)
        if args.change_channel:
            passed('brief channel refusals did not restart discovery',client.count('scanning')==2)
        a.send(c,'relayed after radio reconnection');c.send(a,'return after radio reconnection')
        passed('automatic client reconnect restores configured routes in both directions')
        report['passed']=True
    except Exception as error:
        report['error']=str(error)
        raise
    finally:
        errors=[]
        for adapter in reversed(adapters):
            try:adapter.stop()
            except Exception as error:errors.append('adapter: '+str(error))
        for adapter in adapters:
            try:report['adapters'].append({'node':adapter.node.name,'log':adapter.log,'events':adapter.rows()})
            except Exception as error:errors.append('adapter logs: '+str(error))
        for node in reversed(nodes):
            try:node.cleanup()
            except Exception as error:errors.append(node.name+': '+str(error))
        report['cleanup_errors']=errors
        if errors:report['passed']=False
        args.report.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
        if errors:raise RuntimeError('Cleanup failed: '+'; '.join(errors))


if __name__=='__main__':main()
