#!/usr/bin/env python3
"""A -> Linux BLE relay B -> C over two L2CAP channels; A/C share one host.

Every node/route is loopback-only. SSH manages processes; Bluetooth carries
both relay hops. No power, Wi-Fi, pairing or global discoverability changes.
"""
import argparse
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import subprocess

from lan_demo import Node, require
from linux_ble_demo import Adapter, CONTROL, remote, wait
from macos_ble_adapter import MacAdapter


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args();config=json.loads(args.config.read_text())
    nodes=[Node(spec) for spec in config['nodes']];a,b,c=nodes
    require([n.name for n in nodes]==list('ABC'),'need nodes A/B/C')
    mac=bool(config.get('mac_app'))
    if mac:require(a.ssh is None and b.ssh and c.ssh and b.ssh!=c.ssh,'need local Mac A and separate remote B/C hosts')
    else:require(a.ssh==c.ssh and a.ssh!=b.ssh,'A/C must share one host, B another')
    require(all(n.address=='127.0.0.1' for n in nodes),'nodes must be loopback-only')
    report={'test':'three-host-desktop-ble-relay' if mac else 'two-channel-linux-ble-relay','passed':False,'utc':datetime.now(timezone.utc).isoformat(),
        'topology':'Mac A -> Pi B -> Linux C; both hops LE L2CAP' if mac else 'A/C share endpoint host; B on relay host; A-B and B-C both LE L2CAP',
        'internet_isolation':'not measured; WAN remains connected','checks':[],'adapters':[]}
    adapters=[]
    def passed(name,ok=True):
        require(ok,name);report['checks'].append(name);print('PASS '+name,flush=True)
    def ports(node,count):
        helper='''import socket,json
sockets=[]
try:
 for _ in range('''+str(count)+'''):
  s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s)
 print(json.dumps(['127.0.0.1:'+str(s.getsockname()[1]) for s in sockets]))
finally:
 for s in sockets:s.close()
'''
        result=subprocess.run(node.command(helper),capture_output=True,text=True,check=True,timeout=15)
        return json.loads(result.stdout)
    def start(node,command):
        adapter=Adapter(node,command,len(adapters));adapters.append(adapter);return adapter
    def client(node,local):
        if mac and node is a:
            adapter=MacAdapter(node,config['mac_app'],local,len(adapters));adapters.append(adapter);adapter.start();return adapter
        command=[config['endpoint_ble_binary'],'connect','--local',local,'--node',node.status['listen']]
        if config.get('relay_peer'):command+=['--peer',config['relay_peer']]
        return start(node,command)
    def ready(adapter,local):
        return sum(row.get('event')=='link_up' and row.get('details',{}).get('local')==local for row in adapter.rows())
    def route(source,destination,via):
        source.cli('route','--socket',source.root+'/control.sock','--to',destination.identity['id'],'--via',via)
    def toggle(enabled):
        response=json.loads(remote(b,CONTROL,{'root':b.root,'request':{'command':'relay','enabled':enabled}}))
        require(response['ok'],'relay control failed')
    def text(label):
        value=label
        while len((value+' café 🌍').encode())<=512:value+=' café 🌍'
        return value+'.'*(512-len(value.encode()))
    try:
        for node in nodes:node.prepare()
        report['nodes']=[{'name':n.name,**n.info} for n in nodes]
        for node in nodes:node.start(nodes)
        ba,bc=ports(b,2);la=ports(a,1)[0];lc,excess=ports(c,2)
        if a.ssh==c.ssh:require(len({la,lc,excess})==3,'port allocation collided; repeat the run')
        if mac:
            binary=Path(config['mac_app'])/'Contents/MacOS/pathweft-ble'
            report['mac_bundle_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
            report['mac_build_scope']='preserved earlier authorized central build; app was not rebuilt or re-signed'
        server=start(b,[config['relay_ble_binary'],'serve','--local',ba,'--local',bc,'--node',b.status['listen']])
        wait('relay advertising',lambda:server.count('advertising'))
        first=client(a,la)
        try:wait('first relay channel',lambda:ready(server,ba),20 if mac else 60)
        except TimeoutError:
            if mac and not first.rows():
                report.update(result='blocked',reason='Preserved Mac app produced no Bluetooth callbacks; OS consent may be pending')
                print('BLOCKED '+report['reason'],flush=True);return
            raise
        wait('A local channel',first.local,60)
        second=client(c,lc)
        wait('second relay channel',lambda:ready(server,bc),60);wait('C local channel',second.local,60)
        passed('two simultaneous radio channels opened at distinct loopback next hops')
        for destination in [b,c]:route(a,destination,la)
        for destination in [a,b]:route(c,destination,lc)
        route(b,a,ba);route(b,c,bc)
        outgoing,incoming=text('A via two radio hops to C'),text('C via two radio hops to A')
        a.send(c,outgoing);c.send(a,incoming)
        passed('512-byte text and authenticated receipts crossed both radio hops in both directions')
        for node,expected in [(a,incoming),(c,outgoing)]:
            rows=[json.loads(line) for line in node.call('log')['log'].splitlines()]
            passed(node.name+' recovered exact UTF-8',any(r.get('event')=='text_received' and r['details'].get('text')==expected for r in rows))
        status=b.inspect();report['initial_relay_counters']=status
        passed('B relayed both texts and receipts',status['forwarded']>=4 and status['delivered_texts']==0)
        passed('relay diagnostics contain no relayed private text',all(t not in b.call('log')['log'] for t in [outgoing,incoming]))
        for local in [ba,bc]:
            rows=[r for r in server.rows() if r.get('details',{}).get('local')==local]
            passed('both frame directions used '+local,all(any(r.get('event')==e for r in rows) for e in ['inbound','outbound']))
        extra=client(c,excess)
        wait('excess connection rejected',lambda:server.count('channel_rejected'),45)
        a.send(c,'two links survive an excess connection')
        extra.stop();passed('third connection rejected while both existing channels kept working')
        toggle(False);a.send(c,'relay disabled: must fail',succeeds=False);b.send(c,"relay owner's own message")
        passed('relay opt-out blocks transit while preserving owner messaging');toggle(True)
        before=server.count('channel_closed');first.stop()
        wait('first channel closed',lambda:server.count('channel_closed')>before)
        b.send(c,'second channel remains usable');c.send(b,'return on second channel')
        a.send(c,'first channel is stopped',succeeds=False)
        passed('one channel can stop without breaking the other')
        before=ready(server,ba);first=client(a,la)
        wait('first channel reuses its route port',lambda:ready(server,ba)>before,60)
        wait('A reconnected',first.local,60)
        a.send(c,'relay restored after first channel loss');c.send(a,'return after first channel loss')
        passed('first channel reconnect reuses configured routes and restores two-hop delivery')
        passed('second client remained connected during first-channel recovery',second.count('scanning')==1)
        server.stop();wait('A notices stopped relay',lambda:first.count('link_down'))
        wait('C notices stopped relay',lambda:second.count('link_down'))
        a.send(c,'relay stopped',succeeds=False);c.send(a,'return relay stopped',succeeds=False)
        passed('stopping relay adapter blocks delivery both directions')
        report['final_relay_counters']=b.inspect();report['passed']=True
    except Exception as error:
        report['error']=str(error);raise
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
