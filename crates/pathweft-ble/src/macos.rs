//! macOS (CoreBluetooth) central role: find a Pathweft BLE advertiser, read
//! its L2CAP channel number over GATT, open one persistent LE L2CAP channel and
//! bridge it to the local node with [`crate::bridge::run`].
//!
//! CoreBluetooth callbacks and the channel's streams live on the main run
//! loop. Bytes move to and from the tokio frame bridge through an in-memory
//! duplex pipe. The process must run inside an app bundle that declares
//! `NSBluetoothAlwaysUsageDescription`, or macOS denies Bluetooth access.

use crate::{PSM_CHARACTERISTIC_UUID, SERVICE_UUID, bridge};
use anyhow::Result;
use dispatch2::DispatchQueue;
use objc2::{
    AnyThread, MainThreadMarker, MainThreadOnly, Message, define_class, msg_send, rc::Retained,
    runtime::ProtocolObject,
};
use objc2_core_bluetooth::{
    CBAdvertisementDataLocalNameKey, CBCentralManager, CBCentralManagerDelegate, CBCharacteristic,
    CBL2CAPChannel, CBManagerState, CBPeripheral, CBPeripheralDelegate, CBService, CBUUID,
};
use objc2_foundation::{
    NSArray, NSDefaultRunLoopMode, NSDictionary, NSError, NSInputStream, NSNumber, NSObject,
    NSObjectProtocol, NSOutputStream, NSRunLoop, NSStream, NSStreamDelegate, NSStreamEvent,
    NSString,
};
use std::{cell::RefCell, net::SocketAddr, time::Instant};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

/// A discovered peer must reach an open L2CAP channel within this time, or the attempt is abandoned and scanning resumes.
const CONNECT_TIMEOUT_NS: i64 = 20_000_000_000;

struct Link {
    id: u64,
    peer: Option<Retained<CBPeripheral>>,
    input: Option<Retained<NSInputStream>>,
    output: Option<Retained<NSOutputStream>>,
    _channel: Option<Retained<CBL2CAPChannel>>,
    outbox: Vec<u8>,
    inbound: Option<mpsc::Sender<Vec<u8>>>,
    write_complete: Option<oneshot::Sender<()>>,
}

struct State {
    central: Retained<CBCentralManager>,
    delegate: Retained<Delegate>,
    runtime: Handle,
    local: SocketAddr,
    node: SocketAddr,
    events: bridge::Events,
    started: Instant,
    next_id: u64,
    link: Option<Link>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> Option<R> {
    STATE.with(|cell| cell.borrow_mut().as_mut().map(f))
}

fn uuid(value: &str) -> Retained<CBUUID> {
    unsafe { CBUUID::UUIDWithString(&NSString::from_str(value)) }
}

impl State {
    fn event(&self, name: &str, mut details: serde_json::Value) {
        details["ms"] = serde_json::json!(self.started.elapsed().as_millis() as u64);
        (self.events)(name, details);
    }

    fn scan(&mut self) {
        self.started = Instant::now();
        let services = NSArray::from_retained_slice(&[uuid(SERVICE_UUID)]);
        unsafe {
            self.central
                .scanForPeripheralsWithServices_options(Some(&services), None)
        };
        self.event("scanning", serde_json::json!({}));
    }

    /// Close the current link (if any) and rescan after a short pause.
    fn link_down(&mut self, reason: &str) {
        let Some(link) = self.link.take() else { return };
        for stream in [
            link.input.as_deref().map(|s| s as &NSStream),
            link.output.as_deref().map(|s| s as &NSStream),
        ]
        .into_iter()
        .flatten()
        {
            unsafe {
                stream.setDelegate(None);
                stream.close();
            }
        }
        if let Some(peer) = &link.peer {
            unsafe { self.central.cancelPeripheralConnection(peer) };
        }
        // Dropping `inbound` ends the bridge's link, which drops its loopback port.
        drop(link.inbound);
        self.event("link_down", serde_json::json!({ "reason": reason }));
        DispatchQueue::main()
            .after(dispatch2::DispatchTime::NOW.time(1_000_000_000), || {
                with_state(|s| {
                    if s.link.is_none() && unsafe { s.central.state() } == CBManagerState::PoweredOn
                    {
                        s.scan();
                    }
                });
            })
            .ok();
    }

    fn flush(&mut self) {
        let Some(link) = self.link.as_mut() else {
            return;
        };
        let Some(output) = link.output.clone() else {
            return;
        };
        while !link.outbox.is_empty() && output.hasSpaceAvailable() {
            let written = unsafe {
                output.write_maxLength(
                    std::ptr::NonNull::new(link.outbox.as_mut_ptr()).unwrap(),
                    link.outbox.len(),
                )
            };
            if written <= 0 {
                self.link_down("link write error");
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

    fn read_available(&mut self) {
        let Some(link) = self.link.as_mut() else {
            return;
        };
        let Some(input) = link.input.clone() else {
            return;
        };
        let mut buffer = [0_u8; 8192];
        while input.hasBytesAvailable() {
            let read = unsafe {
                input.read_maxLength(
                    std::ptr::NonNull::new(buffer.as_mut_ptr()).unwrap(),
                    buffer.len(),
                )
            };
            if read <= 0 {
                break;
            }
            if let Some(tx) = &link.inbound
                && tx.try_send(buffer[..read as usize].to_vec()).is_err()
            {
                self.link_down("radio input queue full or closed");
                return;
            }
        }
    }

    /// Wire a freshly opened L2CAP channel to a new frame bridge.
    fn link_up(&mut self, channel: &CBL2CAPChannel) {
        let (input, output) = unsafe { (channel.inputStream(), channel.outputStream()) };
        let (Some(input), Some(output)) = (input, output) else {
            self.link_down("channel without streams");
            return;
        };
        let run_loop = NSRunLoop::mainRunLoop();
        for stream in [&*input as &NSStream, &*output as &NSStream] {
            unsafe {
                stream.setDelegate(Some(ProtocolObject::from_ref(&*self.delegate)));
                stream.scheduleInRunLoop_forMode(&run_loop, NSDefaultRunLoopMode);
                stream.open();
            }
        }
        self.next_id += 1;
        let id = self.next_id;
        // Bytes for the radio are handed to the main thread for the NSOutputStream.
        let outbound = move |chunk: Vec<u8>, done| {
            DispatchQueue::main().exec_async(move || {
                with_state(|s| {
                    if let Some(link) = s.link.as_mut().filter(|l| l.id == id) {
                        link.outbox = chunk;
                        link.write_complete = Some(done);
                        s.flush();
                    }
                });
            });
        };
        // The bridge ended (link error, port bind failure, or our own link_down): tidy up on the main thread.
        let closed = move || {
            DispatchQueue::main().exec_async(move || {
                with_state(|s| {
                    if s.link.as_ref().is_some_and(|l| l.id == id) {
                        s.link_down("bridge ended");
                    }
                });
            });
        };
        let inbound_tx = bridge::spawn_callback_link(
            &self.runtime,
            self.local,
            self.node,
            self.events.clone(),
            outbound,
            closed,
        );
        let link = self.link.get_or_insert_with(|| Link {
            id,
            peer: None,
            input: None,
            output: None,
            _channel: None,
            outbox: Vec::new(),
            inbound: None,
            write_complete: None,
        });
        link.id = id;
        link.input = Some(input);
        link.output = Some(output);
        link._channel = Some(channel.retain());
        link.inbound = Some(inbound_tx);
        self.event("channel_open", serde_json::json!({}));
    }
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "PathweftBleCentralDelegate"]
    struct Delegate;

    unsafe impl NSObjectProtocol for Delegate {}

    unsafe impl CBCentralManagerDelegate for Delegate {
        #[unsafe(method(centralManagerDidUpdateState:))]
        fn did_update_state(&self, central: &CBCentralManager) {
            let state = unsafe { central.state() };
            with_state(|s| {
                s.event("bluetooth_state", serde_json::json!({ "state": state.0 }));
                if state == CBManagerState::PoweredOn {
                    s.scan();
                } else if state == CBManagerState::Unauthorized
                    || state == CBManagerState::Unsupported
                {
                    s.event("bluetooth_unavailable", serde_json::json!({}));
                    std::process::exit(3);
                }
            });
        }

        #[unsafe(method(centralManager:didDiscoverPeripheral:advertisementData:RSSI:))]
        fn did_discover(
            &self,
            central: &CBCentralManager,
            peripheral: &CBPeripheral,
            advertisement: &NSDictionary<NSString, objc2::runtime::AnyObject>,
            rssi: &NSNumber,
        ) {
            let name = unsafe { advertisement.objectForKey(CBAdvertisementDataLocalNameKey) }
                .and_then(|o| o.downcast::<NSString>().ok())
                .map(|n| n.to_string())
                .or_else(|| unsafe { peripheral.name() }.map(|n| n.to_string()));
            with_state(|s| {
                if s.link.is_some() {
                    return;
                }
                s.event(
                    "discovered",
                    serde_json::json!({ "name": name, "rssi": rssi.integerValue() }),
                );
                unsafe {
                    central.stopScan();
                    peripheral.setDelegate(Some(ProtocolObject::from_ref(&*s.delegate)));
                    central.connectPeripheral_options(peripheral, None);
                }
                s.next_id += 1;
                let attempt = s.next_id;
                DispatchQueue::main()
                    .after(
                        dispatch2::DispatchTime::NOW.time(CONNECT_TIMEOUT_NS),
                        move || {
                            with_state(|s| {
                                if s.link
                                    .as_ref()
                                    .is_some_and(|l| l.id == attempt && l.input.is_none())
                                {
                                    s.link_down("connect timeout");
                                }
                            });
                        },
                    )
                    .ok();
                s.link = Some(Link {
                    id: attempt,
                    peer: Some(peripheral.retain()),
                    input: None,
                    output: None,
                    _channel: None,
                    outbox: Vec::new(),
                    inbound: None,
                    write_complete: None,
                });
            });
        }

        #[unsafe(method(centralManager:didConnectPeripheral:))]
        fn did_connect(&self, _central: &CBCentralManager, peripheral: &CBPeripheral) {
            with_state(|s| s.event("connected", serde_json::json!({})));
            let services = NSArray::from_retained_slice(&[uuid(SERVICE_UUID)]);
            unsafe { peripheral.discoverServices(Some(&services)) };
        }

        #[unsafe(method(centralManager:didFailToConnectPeripheral:error:))]
        fn did_fail(
            &self,
            _central: &CBCentralManager,
            _peripheral: &CBPeripheral,
            error: Option<&NSError>,
        ) {
            let reason = format!(
                "connect failed: {:?}",
                error.map(|e| e.localizedDescription().to_string())
            );
            with_state(|s| s.link_down(&reason));
        }

        #[unsafe(method(centralManager:didDisconnectPeripheral:error:))]
        fn did_disconnect(
            &self,
            _central: &CBCentralManager,
            _peripheral: &CBPeripheral,
            error: Option<&NSError>,
        ) {
            let reason = format!(
                "disconnected: {:?}",
                error.map(|e| e.localizedDescription().to_string())
            );
            with_state(|s| s.link_down(&reason));
        }
    }

    unsafe impl CBPeripheralDelegate for Delegate {
        #[unsafe(method(peripheral:didDiscoverServices:))]
        fn did_discover_services(&self, peripheral: &CBPeripheral, _error: Option<&NSError>) {
            let wanted = uuid(SERVICE_UUID);
            let service = unsafe { peripheral.services() }.and_then(|list| {
                list.iter()
                    .find(|svc: &Retained<CBService>| unsafe { svc.UUID() }.isEqual(Some(&wanted)))
            });
            match service {
                Some(service) => {
                    let chars = NSArray::from_retained_slice(&[uuid(PSM_CHARACTERISTIC_UUID)]);
                    unsafe {
                        peripheral.discoverCharacteristics_forService(Some(&chars), &service)
                    };
                }
                None => {
                    with_state(|s| s.link_down("service missing"));
                }
            }
        }

        #[unsafe(method(peripheral:didDiscoverCharacteristicsForService:error:))]
        fn did_discover_characteristics(
            &self,
            peripheral: &CBPeripheral,
            service: &CBService,
            _error: Option<&NSError>,
        ) {
            let wanted = uuid(PSM_CHARACTERISTIC_UUID);
            let found = unsafe { service.characteristics() }.and_then(|list| {
                list.iter().find(|c: &Retained<CBCharacteristic>| {
                    unsafe { c.UUID() }.isEqual(Some(&wanted))
                })
            });
            match found {
                Some(characteristic) => unsafe {
                    peripheral.readValueForCharacteristic(&characteristic)
                },
                None => {
                    with_state(|s| s.link_down("channel-number characteristic missing"));
                }
            }
        }

        #[unsafe(method(peripheral:didUpdateValueForCharacteristic:error:))]
        fn did_update_value(
            &self,
            peripheral: &CBPeripheral,
            characteristic: &CBCharacteristic,
            _error: Option<&NSError>,
        ) {
            let value = unsafe { characteristic.value() }.map(|data| data.to_vec());
            match value.as_deref() {
                Some([low, high]) => {
                    let psm = u16::from_le_bytes([*low, *high]);
                    with_state(|s| s.event("psm", serde_json::json!({ "psm": psm })));
                    unsafe { peripheral.openL2CAPChannel(psm) };
                }
                _ => {
                    with_state(|s| s.link_down("bad channel number"));
                }
            }
        }

        #[unsafe(method(peripheral:didOpenL2CAPChannel:error:))]
        fn did_open_channel(
            &self,
            _peripheral: &CBPeripheral,
            channel: Option<&CBL2CAPChannel>,
            error: Option<&NSError>,
        ) {
            with_state(|s| match channel {
                Some(channel) => s.link_up(channel),
                None => s.link_down(&format!(
                    "L2CAP open failed: {:?}",
                    error.map(|e| e.localizedDescription().to_string())
                )),
            });
        }
    }

    unsafe impl NSStreamDelegate for Delegate {
        #[unsafe(method(stream:handleEvent:))]
        fn handle_event(&self, _stream: &NSStream, event: NSStreamEvent) {
            with_state(|s| {
                if event.contains(NSStreamEvent::HasBytesAvailable) {
                    s.read_available();
                }
                if event.contains(NSStreamEvent::HasSpaceAvailable) {
                    s.flush();
                }
                if event.contains(NSStreamEvent::EndEncountered)
                    || event.contains(NSStreamEvent::ErrorOccurred)
                {
                    s.link_down("link stream ended");
                }
            });
        }
    }
);

impl Delegate {
    fn new(mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(());
        unsafe { msg_send![super(this), init] }
    }
}

/// Run the central role on the current (main) thread until the process exits.
pub fn connect(local: SocketAddr, node: SocketAddr, events: bridge::Events) -> Result<()> {
    let mtm =
        MainThreadMarker::new().ok_or_else(|| anyhow::anyhow!("must run on the main thread"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let delegate = Delegate::new(mtm);
    let central = unsafe {
        CBCentralManager::initWithDelegate_queue(
            CBCentralManager::alloc(),
            Some(ProtocolObject::from_ref(&*delegate)),
            None,
        )
    };
    STATE.with(|cell| {
        *cell.borrow_mut() = Some(State {
            central,
            delegate,
            runtime: runtime.handle().clone(),
            local,
            node,
            events,
            started: Instant::now(),
            next_id: 0,
            link: None,
        })
    });
    NSRunLoop::mainRunLoop().run();
    drop(runtime);
    Ok(())
}
