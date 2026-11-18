#!/usr/bin/env python3
"""Two BLE channels: Linux A -> Mac relay B -> Linux C. A/C share one host.

Requires a built, Bluetooth-authorized Mac peripheral app. No phone, radio
toggle, pairing or global settings change. Writes passed/blocked/failed evidence.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import select
import signal
import socket
import subprocess
import tempfile
import time

from lan_demo import Node, require

ADAPTER_HELPER = r'''
import json,select,signal,subprocess,sys
from pathlib import Path
r=json.loads(sys.stdin.readline())
root=Path(r['root'])
assert root.parent==Path('/tmp') and root.name.startswith('pw-net-')
name=r['log'];assert name.startswith('ble-') and name.endswith('.log') and '/' not in name
with (root/name).open('x') as output:
 p=subprocess.Popen(r['command'],stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT)
print(json.dumps({'pid':p.pid}),flush=True)
try:
 while p.poll() is None:
  if select.select([sys.stdin],[],[],.2)[0] and sys.stdin.readline().strip()!='keep':break
finally:
 if p.poll() is None:
  p.send_signal(signal.SIGINT)
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    linux = config['linux']
    a, c = [Node({**linux, 'name': name, 'address': '127.0.0.1'}) for name in 'AC']
    b = Node({'name': 'B', 'address': '127.0.0.1', 'binary': config['mac_binary']})
    nodes = [a, b, c]
    app = Path(config['mac_app']).resolve(strict=True)
    report = {'test':'two-channel-desktop-ble-relay', 'utc':datetime.now(timezone.utc).isoformat(),
              'result':'failed', 'passed':False, 'checks':[], 'limits':'two physical computers; A/C share Linux; no phone or WAN isolation'}
    adapters = []
    app_pid = None
    def rows(path):
        result = []
        if path.exists():
            for line in path.read_text().splitlines():
                try: result.append(json.loads(line))
                except ValueError: pass
        return result
    def wait(description, predicate, seconds=30):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            value = predicate()
            if value:return value
            time.sleep(.2)
        raise TimeoutError(description)
    def passed(name, condition=True):
        require(condition,name)
        report['checks'].append(name)
        print('PASS '+name,flush=True)
    def route(source, destination, port):
        source.cli('route','--socket',source.root+'/control.sock','--to',destination.identity['id'],'--via',f'127.0.0.1:{port}')
    def relay(enabled):
        helper = """import json,socket,struct,sys
r=json.load(sys.stdin);s=socket.socket(socket.AF_UNIX);s.settimeout(5);s.connect(r['socket'])
b=json.dumps({'command':'relay','enabled':r['enabled']}).encode();s.sendall(struct.pack('>I',len(b))+b)
def read(n):
 b=b''
 while len(b)<n:
  part=s.recv(n-len(b))
  if not part:raise RuntimeError('truncated response')
  b+=part
 return b
n=struct.unpack('>I',read(4))[0];assert 0<n<=32768
print(read(n).decode())
"""
        done = subprocess.run(b.command(helper), input=json.dumps({'socket':b.root+'/control.sock','enabled':enabled}),capture_output=True,text=True,check=True,timeout=10)
        require(json.loads(done.stdout)['ok'],'relay control failed')
    def start_adapter(node, local):
        name=f'ble-{len(adapters)}.log'
        command=[linux['ble_binary'],'connect','--local',f'127.0.0.1:{local}','--node',node.status['listen']]
        if config.get('peer'):command+=['--peer',config['peer']]
        process=subprocess.Popen(node.command(ADAPTER_HELPER),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        adapters.append(process)
        process.stdin.write(json.dumps({'root':node.root,'command':command,'log':name})+'\n');process.stdin.flush()
        require(select.select([process.stdout],[],[],10)[0], 'adapter launcher timed out')
        require('pid' in json.loads(process.stdout.readline()),'adapter did not start')
        def ready():
            helper="import json,sys;from pathlib import Path;r=json.load(sys.stdin);p=Path(r['root']);assert p.parent==Path('/tmp') and p.name.startswith('pw-net-');name=r['log'];assert name.startswith('ble-') and name.endswith('.log') and '/' not in name;print((p/name).read_text())"
            result=subprocess.run(node.command(helper),input=json.dumps({'root':node.root,'log':name}),capture_output=True,text=True,check=True,timeout=10)
            events=[]
            for line in result.stdout.splitlines():
                try:events.append(json.loads(line))
                except ValueError:pass
            return any(r.get('event')=='link_up' and r.get('details',{}).get('local')==f'127.0.0.1:{local}' for r in events)
        wait(node.name+' adapter loopback port',ready,45)
        return process
    def stop_adapter(process):
        if process.poll() is None:
            try:process.stdin.write('stop\n');process.stdin.flush()
            except (BrokenPipeError, OSError):pass
            process.wait(timeout=10)
        for stream in [process.stdin,process.stdout,process.stderr]:stream.close()
    def text(label):
        result=label
        while len((result+' café 🌍').encode())<=512:result+=' café 🌍'
        return result+'.'*(512-len(result.encode()))
    with tempfile.TemporaryDirectory(prefix='pw-dual-run.',dir='/tmp') as temp:
        log=Path(temp)/'mac.log'
        try:
            for node in nodes:node.prepare()
            report['hosts']=[{'name':n.name,**n.info} for n in nodes]
            for node in nodes:node.start(nodes)
            ports=[]
            for _ in range(4):
                with socket.socket() as s:s.bind(('127.0.0.1',0));ports.append(s.getsockname()[1])
            require(len(set(ports))==4,'port allocation collided; repeat the run')
            ba,bc,la,lc=ports
            log.touch()
            subprocess.run(['open','-n',str(app),'--stdout',str(log),'--stderr',str(log),'--args','serve',
                '--local',f'127.0.0.1:{ba}','--local',f'127.0.0.1:{bc}','--node',b.status['listen']],check=True)
            app_pid=wait('Mac process startup',lambda:next((r['details']['pid'] for r in rows(log) if r.get('event')=='process_started'),None),10)
            try:wait('Mac advertising',lambda:any(r.get('event')=='advertising' for r in rows(log)),20)
            except TimeoutError:
                if not any(r.get('event')=='bluetooth_state' for r in rows(log)):
                    report.update(result='blocked', reason='Mac produced no Bluetooth state callback before the deadline; OS consent may be pending')
                    print('BLOCKED: '+report['reason'],flush=True)
                    return
                raise
            count=lambda event:sum(r.get('event')==event for r in rows(log))
            first=start_adapter(a,la)
            wait('first channel',lambda:count('channel_open')>=1 and count('link_up')>=1,45)
            second=start_adapter(c,lc)
            wait('second channel',lambda:count('channel_open')>=2 and count('link_up')>=2,45)
            for endpoint,port in [(a,la),(c,lc)]:
                for destination in nodes:
                    if destination!=endpoint:route(endpoint,destination,port)
            route(b,a,ba);route(b,c,bc)
            outgoing,incoming=text('A -> B -> C'),text('C -> B -> A')
            a.send(c,outgoing);c.send(a,incoming)
            passed('512-byte text and authenticated receipts in both directions')
            for node,expected in [(a,incoming),(c,outgoing)]:
                messages=[json.loads(line) for line in node.call('log')['log'].splitlines()]
                passed(node.name+' recovered exact UTF-8',any(r.get('event')=='text_received' and r['details'].get('text')==expected for r in messages))
            report['relay_counters']=b.inspect()
            passed('Mac relayed both texts and receipts',report['relay_counters']['forwarded']>=4 and report['relay_counters']['delivered_texts']==0)
            passed('relay log contains no message plaintext',not any(t in b.call('log')['log'] for t in [outgoing,incoming]))
            relay(False);a.send(c,'must fail while relay disabled',succeeds=False);b.send(c,"relay owner's message")
            passed('relay opt-out blocks transit but permits owner messaging');relay(True)
            stop_adapter(first)
            wait('first channel closure',lambda:count('channel_closed')>=1)
            b.send(c,'the second link remains usable')
            passed('one link can stop without breaking the other')
            before=count('channel_open');start_adapter(a,la)
            wait('first channel reconnection',lambda:count('channel_open')>before,45)
            a.send(c,'relay after first-link reconnection')
            passed('reconnected link reuses its configured route and relays again')
            report.update(result='passed',passed=True)
        except Exception as error:
            report['error']=str(error)
            raise
        finally:
            errors=[]
            if app_pid:
                command=subprocess.run(['ps','-p',str(app_pid),'-o','command='],capture_output=True,text=True).stdout
                if str(app/'Contents/MacOS/pathweft-ble') in command:
                    try:__import__('os').kill(app_pid,signal.SIGTERM)
                    except ProcessLookupError:pass
            for process in reversed(adapters):
                try:stop_adapter(process)
                except Exception as error:errors.append('adapter: '+str(error))
            report['mac_events']=rows(log)
            for node in reversed(nodes):
                try:node.cleanup()
                except Exception as error:errors.append(node.name+': '+str(error))
            report['cleanup_errors']=errors
            if errors:report.update(result='failed',passed=False)
            args.report.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
            if errors:raise RuntimeError('Cleanup failed: '+'; '.join(errors))


if __name__=='__main__':main()
