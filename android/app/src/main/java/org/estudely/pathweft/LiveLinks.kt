package org.estudely.pathweft

/**
 * Turns Bluetooth slot events into `link_up` / `link_down` control requests for the node.
 *
 * Requests are sent only while discovery is enabled and the node is running. Slots that were
 * already active when discovery turns on are announced then. A slot that goes away is reported
 * only if its `link_up` was reported, so the node never sees a down for a link it never knew.
 */
internal class LiveLinks(private val send: (Request) -> Unit) {
    /** A control request for the node; [kind] is null for `link_down`. */
    data class Request(val command: String, val address: String, val kind: String? = null)

    private val active = linkedSetOf<Int>()
    private var enabled = false

    companion object {
        const val KIND = "ble"
        fun address(port: Int) = "127.0.0.1:$port"
        fun up(port: Int) = Request("link_up", address(port), KIND)
        fun down(port: Int) = Request("link_down", address(port))
    }

    @Synchronized fun setEnabled(on: Boolean) {
        if (on == enabled) return
        enabled = on
        if (on) active.toList().forEach { send(up(it)) }
    }

    @Synchronized fun slotUp(port: Int) {
        if (active.add(port) && enabled) send(up(port))
    }

    @Synchronized fun slotDown(port: Int) {
        if (active.remove(port) && enabled) send(down(port))
    }
}
