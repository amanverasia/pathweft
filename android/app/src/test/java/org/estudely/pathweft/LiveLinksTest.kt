package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test

class LiveLinksTest {
    private fun tracker(sent: MutableList<LiveLinks.Request>) = LiveLinks { sent.add(it) }

    @Test fun requestsUseLoopbackAddressAndBleKind() {
        val up = LiveLinks.up(47101)
        assertEquals("link_up", up.command)
        assertEquals("127.0.0.1:47101", up.address)
        assertEquals("ble", up.kind)
        val down = LiveLinks.down(47101)
        assertEquals("link_down", down.command)
        assertEquals("127.0.0.1:47101", down.address)
        assertNull(down.kind)
    }

    @Test fun nothingIsSentWhileDiscoveryIsOff() {
        val sent = mutableListOf<LiveLinks.Request>()
        val links = tracker(sent)
        links.slotUp(1000)
        links.slotDown(1000)
        assertTrue(sent.isEmpty())
    }

    @Test fun enabledSlotsReportUpAndDownInOrder() {
        val sent = mutableListOf<LiveLinks.Request>()
        val links = tracker(sent)
        links.setEnabled(true)
        links.slotUp(1000); links.slotUp(1001); links.slotDown(1000); links.slotUp(1000)
        assertEquals(listOf("link_up 1000", "link_up 1001", "link_down 1000", "link_up 1000"),
            sent.map { it.command + " " + it.address.substringAfter(':') })
    }

    @Test fun slotsActiveBeforeEnablingAreAnnouncedOnce() {
        val sent = mutableListOf<LiveLinks.Request>()
        val links = tracker(sent)
        links.slotUp(1000); links.slotUp(1001)
        links.setEnabled(true)
        links.setEnabled(true)
        assertEquals(listOf("127.0.0.1:1000", "127.0.0.1:1001"), sent.map { it.address })
    }

    @Test fun duplicateAndUnknownEventsAreIgnored() {
        val sent = mutableListOf<LiveLinks.Request>()
        val links = tracker(sent)
        links.setEnabled(true)
        links.slotDown(1000)
        links.slotUp(1000); links.slotUp(1000)
        links.slotDown(1000); links.slotDown(1000)
        assertEquals(listOf("link_up", "link_down"), sent.map { it.command })
    }

    @Test fun disablingStopsReportsButKeepsTrackingSlots() {
        val sent = mutableListOf<LiveLinks.Request>()
        val links = tracker(sent)
        links.setEnabled(true)
        links.slotUp(1000)
        links.setEnabled(false)
        links.slotDown(1000) // the node is gone; no request is sent
        links.slotUp(1001)
        links.setEnabled(true)
        assertEquals(listOf("127.0.0.1:1000", "127.0.0.1:1001"), sent.map { it.address })
        assertEquals(listOf("link_up", "link_up"), sent.map { it.command })
    }
}
