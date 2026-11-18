//! CoreBluetooth peripheral role. Each accepted channel owns one configured
//! loopback next-hop port; no process-global radio or computer name is changed.
use crate::{SERVICE_UUID, advertised_name, bridge};
use anyhow::{Result, ensure};
use dispatch2::DispatchQueue;
use objc2::{
    AnyThread, MainThreadMarker, MainThreadOnly, Message, define_class, msg_send,
    rc::Retained,
    runtime::{AnyObject, ProtocolObject},
};
use objc2_core_bluetooth::{
    CBAdvertisementDataLocalNameKey, CBAdvertisementDataServiceUUIDsKey, CBL2CAPChannel,
    CBManagerState, CBPeripheralManager, CBPeripheralManagerDelegate, CBUUID,
};
use objc2_foundation::{
    NSArray, NSDefaultRunLoopMode, NSDictionary, NSError, NSInputStream, NSObject,
    NSObjectProtocol, NSOutputStream, NSRunLoop, NSStream, NSStreamDelegate, NSStreamEvent,
    NSString,
};
use std::{cell::RefCell, collections::BTreeMap, net::SocketAddr, sync::Arc};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

struct Link {
    local: SocketAddr,
    input: Retained<NSInputStream>,
    output: Retained<NSOutputStream>,
    _channel: Retained<CBL2CAPChannel>,
    inbound: mpsc::Sender<Vec<u8>>,
    write_complete: Option<oneshot::Sender<()>>,
    outbox: Vec<u8>,
}
struct State {
    manager: Retained<CBPeripheralManager>,
    delegate: Retained<Delegate>,
    runtime: Handle,
    locals: Vec<SocketAddr>,
    node: SocketAddr,
    events: bridge::Events,
    links: BTreeMap<u64, Link>,
    next_id: u64,
    published: bool,
}
thread_local! { static STATE: RefCell<Option<State>> = const { RefCell::new(None) }; }
fn with_state<R>(action: impl FnOnce(&mut State) -> R) -> Option<R> {
    STATE.with(|cell| cell.borrow_mut().as_mut().map(action))
}

impl State {
    fn event(&self, event: &str, details: serde_json::Value) {
        (self.events)(event, details);
    }

    fn close(&mut self, id: u64, reason: &str) {
        let Some(link) = self.links.remove(&id) else {
            return;
        };
        for stream in [&*link.input as &NSStream, &*link.output as &NSStream] {
            unsafe {
                stream.setDelegate(None);
                stream.removeFromRunLoop_forMode(&NSRunLoop::mainRunLoop(), NSDefaultRunLoopMode);
                stream.close();
            }
        }
        drop(link.inbound);
        self.event(
            "channel_closed",
            serde_json::json!({"id": id, "local":link.local, "reason":reason}),
        );
    }

    fn flush(&mut self, id: u64) {
        let Some(link) = self.links.get_mut(&id) else {
            return;
        };
        while !link.outbox.is_empty() && link.output.hasSpaceAvailable() {
            let written = unsafe {
                link.output.write_maxLength(
                    std::ptr::NonNull::new(link.outbox.as_mut_ptr()).unwrap(),
                    link.outbox.len(),
                )
            };
            if written <= 0 {
                self.close(id, "radio write failed");
                return;
            }
            link.outbox.drain(..written as usize);
        }
        if link.outbox.is_empty()
            && let Some(done) = link.write_complete.take()
        {
            let _ = done.send(());
        }
    }

    fn read(&mut self, id: u64) {
        let Some(link) = self.links.get(&id) else {
            return;
        };
        let mut buffer = [0u8; 8192];
        while link.input.hasBytesAvailable() {
            let count = unsafe {
                link.input.read_maxLength(
                    std::ptr::NonNull::new(buffer.as_mut_ptr()).unwrap(),
                    buffer.len(),
                )
            };
            if count <= 0 {
                self.close(id, "radio read ended");
                return;
            }
            if link
                .inbound
                .try_send(buffer[..count as usize].to_vec())
                .is_err()
            {
                self.close(id, "bridge reader ended");
                return;
            }
        }
    }

    fn accept(&mut self, channel: &CBL2CAPChannel) {
        let (input, output) = unsafe { (channel.inputStream(), channel.outputStream()) };
        let (Some(input), Some(output)) = (input, output) else {
            self.event(
                "channel_rejected",
                serde_json::json!({"reason":"missing streams"}),
            );
            return;
        };
        let local = self
            .locals
            .iter()
            .find(|port| !self.links.values().any(|link| link.local == **port))
            .copied();
        let Some(local) = local else {
            input.close();
            output.close();
            self.event(
                "channel_rejected",
                serde_json::json!({"reason":"link limit"}),
            );
            return;
        };
        self.next_id += 1;
        let id = self.next_id;
        for stream in [&*input as &NSStream, &*output as &NSStream] {
            unsafe {
                stream.setDelegate(Some(ProtocolObject::from_ref(&*self.delegate)));
                stream.scheduleInRunLoop_forMode(&NSRunLoop::mainRunLoop(), NSDefaultRunLoopMode);
                stream.open();
            }
        }
        let sink = self.events.clone();
        let link_events: bridge::Events = Arc::new(move |event, mut details| {
            details["channel_id"] = serde_json::json!(id);
            details["local"] = serde_json::json!(local);
            sink(event, details);
        });
        let inbound = bridge::spawn_callback_link(
            &self.runtime,
            local,
            self.node,
            link_events,
            move |bytes, done| {
                DispatchQueue::main().exec_async(move || {
                    with_state(|s| {
                        if let Some(link) = s.links.get_mut(&id) {
                            link.outbox = bytes;
                            link.write_complete = Some(done);
                            s.flush(id);
                        }
                    });
                });
            },
            move || {
                DispatchQueue::main().exec_async(move || {
                    with_state(|s| s.close(id, "bridge ended"));
                });
            },
        );
        self.links.insert(
            id,
            Link {
                local,
                input,
                output,
                _channel: channel.retain(),
                inbound,
                outbox: Vec::new(),
                write_complete: None,
            },
        );
        self.event("channel_open", serde_json::json!({"id":id, "local":local}));
    }
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "PathweftBlePeripheralDelegate"]
    struct Delegate;
    unsafe impl NSObjectProtocol for Delegate {}
    unsafe impl CBPeripheralManagerDelegate for Delegate {
        #[unsafe(method(peripheralManagerDidUpdateState:))]
        fn state_changed(&self, manager: &CBPeripheralManager) {
            let state = unsafe { manager.state() };
            with_state(|s| {
                s.event("bluetooth_state", serde_json::json!({"state":state.0}));
                if state == CBManagerState::PoweredOn {
                    if !s.published {
                        s.published = true;
                        unsafe {
                            manager.publishL2CAPChannelWithEncryption(false);
                        }
                    }
                } else {
                    for id in s.links.keys().copied().collect::<Vec<_>>() {
                        s.close(id, "Bluetooth unavailable");
                    }
                    s.published = false;
                    if state == CBManagerState::Unauthorized || state == CBManagerState::Unsupported
                    {
                        s.event("bluetooth_unavailable", serde_json::json!({}));
                        std::process::exit(3);
                    }
                }
            });
        }
        #[unsafe(method(peripheralManager:didPublishL2CAPChannel:error:))]
        fn published(&self, manager: &CBPeripheralManager, psm: u16, error: Option<&NSError>) {
            if let Some(error) = error {
                with_state(|s| {
                    s.event(
                        "publish_failed",
                        serde_json::json!({"error":error.localizedDescription().to_string()}),
                    )
                });
                return;
            }
            let name = NSString::from_str(&advertised_name(psm));
            let uuid = unsafe { CBUUID::UUIDWithString(&NSString::from_str(SERVICE_UUID)) };
            let services = NSArray::from_retained_slice(&[uuid]);
            let data = unsafe {
                NSDictionary::<NSString, AnyObject>::from_slices(
                    &[
                        CBAdvertisementDataLocalNameKey,
                        CBAdvertisementDataServiceUUIDsKey,
                    ],
                    &[&*name as &AnyObject, &*services as &AnyObject],
                )
            };
            unsafe {
                manager.startAdvertising(Some(&data));
            }
            with_state(|s| {
                s.event(
                    "published",
                    serde_json::json!({"psm":psm,"name":name.to_string()}),
                )
            });
        }
        #[unsafe(method(peripheralManagerDidStartAdvertising:error:))]
        fn advertising(&self, _manager: &CBPeripheralManager, error: Option<&NSError>) {
            with_state(|s| {
                s.event(if error.is_none() { "advertising" } else { "advertising_failed" },
                serde_json::json!({"error":error.map(|e| e.localizedDescription().to_string())}))
            });
        }
        #[unsafe(method(peripheralManager:didOpenL2CAPChannel:error:))]
        fn accepted(
            &self,
            _manager: &CBPeripheralManager,
            channel: Option<&CBL2CAPChannel>,
            error: Option<&NSError>,
        ) {
            with_state(|s| {
                if let Some(channel) = channel {
                    s.accept(channel);
                } else {
                    s.event("channel_failed", serde_json::json!({"error":error.map(|e|e.localizedDescription().to_string())}));
                }
            });
        }
    }
    unsafe impl NSStreamDelegate for Delegate {
        #[unsafe(method(stream:handleEvent:))]
        fn stream_event(&self, stream: &NSStream, event: NSStreamEvent) {
            with_state(|s| {
                let id = s.links.iter().find_map(|(&id, link)| {
                    (std::ptr::eq(&*link.input as &NSStream, stream)
                        || std::ptr::eq(&*link.output as &NSStream, stream))
                    .then_some(id)
                });
                let Some(id) = id else { return };
                if event.contains(NSStreamEvent::ErrorOccurred)
                    || event.contains(NSStreamEvent::EndEncountered)
                {
                    s.close(id, "radio stream ended");
                    return;
                }
                if event.contains(NSStreamEvent::HasBytesAvailable) {
                    s.read(id);
                }
                if event.contains(NSStreamEvent::HasSpaceAvailable) {
                    s.flush(id);
                }
            });
        }
    }
);
impl Delegate {
    fn new(marker: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(marker).set_ivars(());
        unsafe { msg_send![super(this), init] }
    }
}

pub fn serve(locals: Vec<SocketAddr>, node: SocketAddr, events: bridge::Events) -> Result<()> {
    ensure!(
        (1..=2).contains(&locals.len()),
        "provide one or two --local ports"
    );
    ensure!(
        locals.iter().all(|p| p.port() != 0 && p.ip().is_loopback()),
        "local ports must be fixed loopback ports"
    );
    ensure!(
        locals.len() == 1 || locals[0] != locals[1],
        "local ports must be distinct"
    );
    let marker =
        MainThreadMarker::new().ok_or_else(|| anyhow::anyhow!("must run on main thread"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let delegate = Delegate::new(marker);
    let manager = unsafe {
        CBPeripheralManager::initWithDelegate_queue(
            CBPeripheralManager::alloc(),
            Some(ProtocolObject::from_ref(&*delegate)),
            None,
        )
    };
    STATE.with(|cell| {
        *cell.borrow_mut() = Some(State {
            manager,
            delegate,
            runtime: runtime.handle().clone(),
            locals,
            node,
            events,
            links: BTreeMap::new(),
            next_id: 0,
            published: false,
        })
    });
    NSRunLoop::mainRunLoop().run();
    // Own the manager for the lifetime of its callbacks.
    STATE.with(|cell| {
        if let Some(state) = cell.borrow_mut().take() {
            unsafe {
                state.manager.stopAdvertising();
            }
        }
    });
    drop(runtime);
    Ok(())
}
