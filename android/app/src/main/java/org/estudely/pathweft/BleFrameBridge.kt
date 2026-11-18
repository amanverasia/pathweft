package org.estudely.pathweft

import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/** One radio stream, a bounded frame queue, and a loopback-only next-hop port. */
internal class BleFrameBridge(
    private val input: InputStream,
    private val output: OutputStream,
    private val radio: Closeable,
    private val node: InetSocketAddress,
    private val event: (String) -> Unit,
    private val closed: () -> Unit,
    bindPort: Int = 0,
) : Closeable {
    init {
        require(node.address?.isLoopbackAddress == true && node.port != 0)
        require(bindPort in 0..65535)
    }
    private val stopping = AtomicBoolean(false)
    private val listener = ServerSocket().apply {
        try {
            reuseAddress = true
            bind(InetSocketAddress(InetAddress.getByName("127.0.0.1"), bindPort), 4)
        } catch (failure: Exception) { close(); throw failure }
    }
    val localPort: Int get() = listener.localPort
    private val queue = ArrayBlockingQueue<ByteArray>(8)
    private val sockets = ConcurrentHashMap.newKeySet<Socket>()
    private val factory = ThreadFactory { task -> Thread(task, "pathweft-ble-io").apply { isDaemon = true } }
    private val readers = ThreadPoolExecutor(2, 2, 0L, TimeUnit.SECONDS, ArrayBlockingQueue(4), factory)
    private val watchdog = Executors.newSingleThreadScheduledExecutor(factory)

    fun start() {
        thread(name = "pathweft-ble-accept", isDaemon = true) {
            try {
                while (!stopping.get()) {
                    val socket = listener.accept().apply { soTimeout = 2000 }
                    sockets.add(socket)
                    try {
                        readers.execute {
                            try {
                                socket.use {
                                    val frame = BleFrames.read(it.getInputStream()) ?: return@execute
                                    if (!queue.offer(frame)) event("outbound_queue_full")
                                }
                            } catch (_: Exception) { if (!stopping.get()) event("outbound_rejected") }
                            finally { sockets.remove(socket) }
                        }
                    } catch (_: RejectedExecutionException) { sockets.remove(socket); socket.close() }
                }
            } catch (_: Exception) { close() }
        }
        thread(name = "pathweft-ble-reader", isDaemon = true) {
            try {
                while (!stopping.get()) {
                    val frame = BleFrames.read(input) ?: break
                    // Only the phone's own loopback node can receive radio frames.
                    val socket = Socket()
                    sockets.add(socket)
                    try {
                        socket.use {
                            it.connect(node, 2000)
                            BleFrames.write(it.getOutputStream(), frame)
                        }
                        event("inbound")
                    } catch (_: Exception) { if (!stopping.get()) event("inbound_failed") }
                    finally { sockets.remove(socket) }
                }
            } catch (_: Exception) { if (!stopping.get()) event("link_read_failed") }
            finally { close() }
        }
        thread(name = "pathweft-ble-writer", isDaemon = true) {
            try {
                while (!stopping.get()) {
                    val frame = queue.poll(1, TimeUnit.SECONDS) ?: continue
                    val deadline = watchdog.schedule({ close() }, 10, TimeUnit.SECONDS)
                    try { BleFrames.write(output, frame); event("outbound") }
                    finally { deadline.cancel(false) }
                }
            } catch (_: Exception) { if (!stopping.get()) event("link_write_failed") }
            finally { close() }
        }
    }

    override fun close() {
        if (!stopping.compareAndSet(false, true)) return
        runCatching { listener.close() }
        runCatching { radio.close() } // Unblocks Bluetooth reads and writes.
        readers.shutdownNow()
        watchdog.shutdownNow()
        sockets.forEach { runCatching { it.close() } }
        queue.clear()
        closed()
    }
}
