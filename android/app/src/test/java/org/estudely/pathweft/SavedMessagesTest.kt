package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test

class SavedMessagesTest {
    private fun entry(state: String) = OutgoingEntry(
        id = "0123456789abcdef0123456789abcdef",
        recipient = "fedcba9876543210fedcba9876543210",
        state = state,
        attempts = 2,
        receiptQueries = 1,
        redeliveries = 3,
        lastError = "receipt_timeout",
    )

    @Test fun outgoingActionsMatchEveryDeliveryState() {
        val expected = mapOf(
            "queued" to Pair(true, false),
            "in_flight" to Pair(false, false),
            "unconfirmed" to Pair(false, true),
            "delivered" to Pair(false, true),
            "expired" to Pair(false, true),
            "cancelled" to Pair(false, true),
            "unexpected" to Pair(false, false),
        )
        for ((state, actions) in expected) {
            val entry = entry(state)
            assertEquals(state, actions.first, entry.canCancel)
            assertEquals(state, actions.second, entry.canForget)
        }
    }

    @Test fun outgoingLabelsContainOnlyPublicMetadata() {
        val entry = entry("unconfirmed")
        assertEquals("Unconfirmed · 0123456789ab…", entry.headline())
        assertEquals(
            "To fedcba987654…\nAttempts 2 · Queries 1 · Redeliveries 3\nLast error: receipt timeout",
            entry.details(),
        )
        assertEquals("In flight", SavedMessages.stateLabel("in_flight"))
    }

    @Test fun incomingLabelsShortenIdentifiersWithoutNeedingText() {
        val entry = IncomingEntry("0123456789abcdef", "fedcba9876543210")
        assertEquals("From fedcba987654…\nMessage 0123456789ab…", entry.headline())
        assertEquals("short", SavedMessages.shortId("short"))
    }
}
