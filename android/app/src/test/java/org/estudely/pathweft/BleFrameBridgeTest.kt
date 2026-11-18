package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class BleFrameBridgeTest {
    @Test fun closingOneChannelLeavesTheOtherFramePathWorking() {
        ServerSocket(0).use { node ->
            node.soTimeout = 2000
            val resources = mutableListOf<java.io.Closeable>()
            val peers = mutableListOf<Socket>()
            val bridges = mutableListOf<BleFrameBridge>()
            val closed = List(2) { CountDownLatch(1) }
            try {
                repeat(2) { index ->
                    ServerSocket(0).use { server ->
                        val peer = Socket("127.0.0.1", server.localPort).apply { soTimeout = 2000 }
                        resources.add(peer); peers.add(peer)
                        val radio = server.accept(); resources.add(radio)
                        val bridge = BleFrameBridge(radio.getInputStream(), radio.getOutputStream(), radio,
                            InetSocketAddress("127.0.0.1", node.localPort), {}, { closed[index].countDown() })
                        resources.add(bridge); bridges.add(bridge); bridge.start()
                    }
                }
                assertNotEquals(bridges[0].localPort, bridges[1].localPort)
                bridges[0].close()
                assertTrue(closed[0].await(2, TimeUnit.SECONDS))
                assertThrows(java.io.IOException::class.java) { Socket("127.0.0.1", bridges[0].localPort).close() }
                val body = ByteArray(BleFrames.MAX_BYTES) { (it % 251).toByte() }
                Socket("127.0.0.1", bridges[1].localPort).use { BleFrames.write(it.getOutputStream(), body) }
                assertArrayEquals(body, BleFrames.read(peers[1].getInputStream()))
                BleFrames.write(peers[1].getOutputStream(), "second channel survives".toByteArray())
                node.accept().use { delivered ->
                    delivered.soTimeout = 2000
                    assertEquals("second channel survives", String(BleFrames.read(delivered.getInputStream())!!))
                }
                assertEquals(1L, closed[1].count)
                bridges[1].close()
                assertTrue(closed[1].await(2, TimeUnit.SECONDS))
            } finally { resources.asReversed().forEach { runCatching { it.close() } } }
        }
    }

    @Test fun bothDirectionsAndRadioLossCloseTheNextHop() {
        ServerSocket(0).use { radioServer ->
            Socket("127.0.0.1", radioServer.localPort).use { peer ->
                radioServer.accept().use { radio ->
                    ServerSocket(0).use { node ->
                        node.soTimeout = 2000
                        peer.soTimeout = 2000
                        val closed = CountDownLatch(1)
                        val bridge = BleFrameBridge(radio.getInputStream(), radio.getOutputStream(), radio,
                            InetSocketAddress("127.0.0.1", node.localPort), {}, { closed.countDown() })
                        try {
                            bridge.start()
                            val body = ByteArray(BleFrames.MAX_BYTES) { (it % 256).toByte() }
                            Socket("127.0.0.1", bridge.localPort).use { BleFrames.write(it.getOutputStream(), body) }
                            assertArrayEquals(body, BleFrames.read(peer.getInputStream()))
                            BleFrames.write(peer.getOutputStream(), "from radio".toByteArray())
                            node.accept().use { delivered ->
                                delivered.soTimeout = 2000
                                assertEquals("from radio", String(BleFrames.read(delivered.getInputStream())!!))
                            }
                            val local = bridge.localPort
                            peer.close()
                            assertTrue("radio EOF closes bridge", closed.await(2, TimeUnit.SECONDS))
                            assertThrows(java.io.IOException::class.java) { Socket("127.0.0.1", local).close() }
                        } finally { bridge.close() }
                    }
                }
            }
        }
    }
    @Test fun explicitStopClosesIdleConnectionsAndRadio() {
        ServerSocket(0).use { server ->
            Socket("127.0.0.1", server.localPort).use { peer ->
                server.accept().use { radio ->
                    val stopped = CountDownLatch(1)
                    val bridge = BleFrameBridge(radio.getInputStream(), radio.getOutputStream(), radio,
                        InetSocketAddress("127.0.0.1", 1), {}, { stopped.countDown() })
                    bridge.start()
                    Socket("127.0.0.1", bridge.localPort).use { stalled ->
                        stalled.soTimeout = 2000
                        bridge.close()
                        assertTrue(stopped.await(2, TimeUnit.SECONDS))
                        assertTrue(radio.isClosed)
                        assertEquals(-1, stalled.getInputStream().read())
                        bridge.close() // Idempotent cleanup.
                    }
                }
            }
        }
    }
}
