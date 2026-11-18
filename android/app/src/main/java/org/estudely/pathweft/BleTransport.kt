package org.estudely.pathweft

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.os.ParcelUuid
import org.json.JSONArray
import org.json.JSONObject
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/** Android's LE L2CAP peripheral. No pairing, device renaming or radio toggles. */
@SuppressLint("MissingPermission") // Every start checks runtime permissions before Bluetooth access.
internal class BleTransport(private val context: Context, private val slotEvents: LiveLinks? = null) {
    companion object {
        val SERVICE: UUID = UUID.fromString("7e1f0001-5a7f-4c3b-9d1e-70a7f0e5b001")
        val PSM_CHARACTERISTIC: UUID = UUID.fromString("7e1f0002-5a7f-4c3b-9d1e-70a7f0e5b001")
        const val MAX_LINKS = 2
    }
    private val lock = Any()
    private var epoch = 0L
    private var listener: BluetoothServerSocket? = null
    private var gatt: BluetoothGattServer? = null
    private var advertiser: BluetoothLeAdvertiser? = null
    private var advertisement: AdvertiseCallback? = null
    private var advertising = false
    private var psm = 0
    private var error: String? = null
    private var nextLink = 0L
    private var slots: BleLinkSlots? = null
    private class Link(val address: String) {
        var bridge: BleFrameBridge? = null
        val inbound = AtomicLong()
        val outbound = AtomicLong()
    }
    private val links = linkedMapOf<Long, Link>()

    fun status(): JSONObject = synchronized(lock) {
        JSONObject().put("listening", listener != null).put("advertising", advertising).put("psm", psm)
            .put("error", error ?: JSONObject.NULL).put("links", JSONArray(links.map { (id, link) ->
                JSONObject().put("id", id).put("address", link.address)
                    .put("local", link.bridge?.let { "127.0.0.1:${it.localPort}" } ?: JSONObject.NULL)
                    .put("inbound", link.inbound.get()).put("outbound", link.outbound.get())
            }))
    }

    fun listen(nodePort: Int): JSONObject = synchronized(lock) {
        if (Build.VERSION.SDK_INT < 29) error("Bluetooth links need Android 10 or later")
        if (listener != null) return@synchronized status()
        if (Build.VERSION.SDK_INT >= 31) {
            for (permission in arrayOf(Manifest.permission.BLUETOOTH_CONNECT, Manifest.permission.BLUETOOTH_ADVERTISE)) {
                check(context.checkSelfPermission(permission) == PackageManager.PERMISSION_GRANTED) { "Allow Nearby devices for Bluetooth first" }
            }
        }
        require(nodePort in 1..65535)
        val manager = context.getSystemService(BluetoothManager::class.java)
        val adapter = checkNotNull(manager.adapter) { "No Bluetooth adapter" }
        check(adapter.isEnabled) { "Turn on Bluetooth first" }
        val le = checkNotNull(adapter.bluetoothLeAdvertiser) { "Bluetooth advertising is unavailable" }
        val linkSlots = slots ?: BleLinkSlots(MAX_LINKS).also { slots = it }
        val server = adapter.listenUsingInsecureL2capChannel()
        val generation = ++epoch
        listener = server
        psm = server.psm
        error = null
        val channelBytes = byteArrayOf((psm and 255).toByte(), (psm shr 8).toByte())
        try {
            val callback = object : AdvertiseCallback() {
                override fun onStartSuccess(settings: AdvertiseSettings) = synchronized(lock) {
                    if (generation == epoch) advertising = true
                }
                override fun onStartFailure(errorCode: Int) = fail(generation, "Bluetooth advertising failed ($errorCode)")
            }
            advertiser = le
            advertisement = callback
            val gattCallback = object : BluetoothGattServerCallback() {
                override fun onServiceAdded(status: Int, service: BluetoothGattService) {
                    synchronized(lock) {
                        if (generation != epoch) return
                        if (status != BluetoothGatt.GATT_SUCCESS) { fail(generation, "Bluetooth service registration failed ($status)"); return }
                        // Keep each legacy packet below 31 bytes. The service UUID
                        // is in the main packet; its channel data is in the scan response.
                        val data = AdvertiseData.Builder().addServiceUuid(ParcelUuid(SERVICE)).build()
                        val response = AdvertiseData.Builder().addServiceData(ParcelUuid(SERVICE), channelBytes).build()
                        try {
                            le.startAdvertising(AdvertiseSettings.Builder().setConnectable(true)
                                .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY).build(), data, response, callback)
                        } catch (failure: Exception) { fail(generation, failure.message ?: "Bluetooth advertising failed") }
                    }
                }
                override fun onCharacteristicReadRequest(device: BluetoothDevice, requestId: Int, offset: Int, characteristic: BluetoothGattCharacteristic) {
                    synchronized(lock) {
                        if (generation != epoch) return
                        val valid = characteristic.uuid == PSM_CHARACTERISTIC && offset in 0..channelBytes.size
                        gatt?.sendResponse(device, requestId,
                            if (valid) BluetoothGatt.GATT_SUCCESS else BluetoothGatt.GATT_INVALID_OFFSET,
                            offset, if (valid) channelBytes.copyOfRange(offset, channelBytes.size) else null)
                    }
                }
            }
            val service = BluetoothGattService(SERVICE, BluetoothGattService.SERVICE_TYPE_PRIMARY)
            service.addCharacteristic(BluetoothGattCharacteristic(PSM_CHARACTERISTIC,
                BluetoothGattCharacteristic.PROPERTY_READ, BluetoothGattCharacteristic.PERMISSION_READ))
            gatt = checkNotNull(manager.openGattServer(context, gattCallback)) { "Could not open Bluetooth service" }
            check(gatt!!.addService(service)) { "Could not register Bluetooth service" }
            thread(name = "pathweft-ble-listen", isDaemon = true) {
                try {
                    while (true) {
                        val socket = server.accept()
                        synchronized(lock) {
                            if (generation != epoch || links.size >= MAX_LINKS) { socket.close(); return@synchronized }
                            val lease = linkSlots.claim()
                            if (lease == null) { socket.close(); return@synchronized }
                            val id = ++nextLink
                            val link = Link(socket.remoteDevice.address)
                            try {
                                val bridge = BleFrameBridge(socket.inputStream, socket.outputStream, socket,
                                    InetSocketAddress("127.0.0.1", nodePort),
                                    { event -> when (event) { "inbound" -> link.inbound.incrementAndGet(); "outbound" -> link.outbound.incrementAndGet() } },
                                    { synchronized(lock) { links.remove(id); if (linkSlots.release(lease)) slotEvents?.slotDown(lease.port) } },
                                    bindPort = lease.port)
                                link.bridge = bridge
                                links[id] = link
                                bridge.start()
                                slotEvents?.slotUp(lease.port) // Its loopback port is now listening.
                            } catch (failure: Exception) {
                                link.bridge?.close()
                                links.remove(id)
                                linkSlots.release(lease)
                                runCatching { socket.close() }
                                error = failure.message
                            }
                        }
                    }
                } catch (failure: Exception) { fail(generation, failure.message ?: "Bluetooth listener stopped") }
            }
            status()
        } catch (failure: Exception) { stop(); throw failure }
    }

    private fun fail(generation: Long, message: String) = synchronized(lock) {
        if (generation == epoch) { stop(); error = message }
    }

    fun stop() = synchronized(lock) {
        ++epoch // Late callbacks from an older session cannot affect a new one.
        advertising = false
        advertisement?.let { callback -> runCatching { advertiser?.stopAdvertising(callback) } }
        advertisement = null
        advertiser = null
        runCatching { gatt?.close() }; gatt = null
        runCatching { listener?.close() }; listener = null
        links.values.mapNotNull { it.bridge }.toList().forEach { it.close() }
        links.clear()
        psm = 0
    }
}
