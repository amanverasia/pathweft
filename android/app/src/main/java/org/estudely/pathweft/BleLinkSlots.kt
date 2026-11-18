package org.estudely.pathweft

import java.net.InetAddress
import java.net.ServerSocket

/** Stable next-hop addresses for this process; an unused slot has no open listener. */
internal class BleLinkSlots(capacity: Int) {
    class Lease internal constructor(val port: Int)

    private val ports: List<Int>
    private val occupied = mutableMapOf<Int, Lease>()

    init {
        require(capacity in 1..2)
        val reservations = mutableListOf<ServerSocket>()
        try {
            // Hold all sockets until allocation finishes so the ports are distinct.
            repeat(capacity) {
                reservations.add(ServerSocket(0, 4, InetAddress.getByName("127.0.0.1")))
            }
            ports = reservations.map { it.localPort }
        } finally {
            reservations.forEach { runCatching { it.close() } }
        }
    }

    @Synchronized fun claim(): Lease? {
        val port = ports.firstOrNull { it !in occupied } ?: return null
        return Lease(port).also { occupied[port] = it }
    }

    /** Returns true only if this lease was still the current holder of its port. */
    @Synchronized fun release(lease: Lease): Boolean {
        // A late close from an old connection must not release its replacement.
        if (occupied[lease.port] !== lease) return false
        occupied.remove(lease.port)
        return true
    }
}
