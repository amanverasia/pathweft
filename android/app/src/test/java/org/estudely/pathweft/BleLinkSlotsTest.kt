package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test
import java.io.Closeable
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class BleLinkSlotsTest {
    @Test fun replacementUsesTheExistingRouteWhileTheOtherLinkStaysUsable() {
        val slots = BleLinkSlots(2)
        val first = slots.claim()!!
        val second = slots.claim()!!
        assertNotEquals(first.port, second.port)
        assertNull("two links exhaust capacity", slots.claim())
        ServerSocket(0, 4, InetAddress.getByName("127.0.0.1")).use { node ->
            node.soTimeout = 2000
            val resources = mutableListOf<Closeable>()
            fun open(lease: BleLinkSlots.Lease, closed: CountDownLatch): Pair<BleFrameBridge, Socket> {
                ServerSocket(0, 4, InetAddress.getByName("127.0.0.1")).use { server ->
                    val peer = Socket("127.0.0.1", server.localPort).apply { soTimeout = 2000 }
                    resources.add(peer)
                    val radio = server.accept(); resources.add(radio)
                    val bridge = BleFrameBridge(radio.getInputStream(), radio.getOutputStream(), radio,
                        InetSocketAddress("127.0.0.1", node.localPort), {},
                        { slots.release(lease); closed.countDown() }, bindPort = lease.port)
                    resources.add(bridge)
                    assertEquals(lease.port, bridge.localPort)
                    bridge.start()
                    return bridge to peer
                }
            }
            fun send(port: Int, body: ByteArray, peer: Socket) {
                Socket("127.0.0.1", port).use { BleFrames.write(it.getOutputStream(), body) }
                assertArrayEquals(body, BleFrames.read(peer.getInputStream()))
            }
            try {
                val firstClosed = CountDownLatch(1)
                val (original, oldPeer) = open(first, firstClosed)
                val secondClosed = CountDownLatch(1)
                val (other, otherPeer) = open(second, secondClosed)
                val routePort = original.localPort
                val body = ByteArray(BleFrames.MAX_BYTES) { (it % 251).toByte() }
                send(routePort, body, oldPeer)

                oldPeer.close() // Radio loss frees only this slot and closes its next hop.
                assertTrue(firstClosed.await(2, TimeUnit.SECONDS))
                assertThrows(IOException::class.java) { Socket("127.0.0.1", routePort).close() }
                send(second.port, body, otherPeer)
                assertEquals(second.port, other.localPort)
                assertEquals(1L, secondClosed.count)

                val replacement = slots.claim()!!
                assertEquals(routePort, replacement.port)
                slots.release(first) // Simulate a delayed callback from the old connection.
                assertNull("old release cannot free the new lease", slots.claim())
                val replacementClosed = CountDownLatch(1)
                val (reconnected, newPeer) = open(replacement, replacementClosed)
                send(routePort, body, newPeer) // The caller keeps the original route address.
                BleFrames.write(newPeer.getOutputStream(), body)
                node.accept().use { delivered ->
                    delivered.soTimeout = 2000
                    assertArrayEquals(body, BleFrames.read(delivered.getInputStream()))
                }
                send(second.port, "other link still active".toByteArray(), otherPeer)
                reconnected.close()
                assertTrue(replacementClosed.await(2, TimeUnit.SECONDS))
                assertThrows(IOException::class.java) { Socket("127.0.0.1", routePort).close() }
                assertEquals(1L, secondClosed.count)
            } finally { resources.asReversed().forEach { runCatching { it.close() } } }
        }
    }
}
