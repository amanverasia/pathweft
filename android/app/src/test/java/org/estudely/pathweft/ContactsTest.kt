package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test

class ContactsTest {
    private val alice = "a".repeat(64)
    private val bob = "b".repeat(64)
    private val impostor = "c".repeat(64)

    @Test fun newKeyIsAddedWithoutReorderingOrReplacingExistingContacts() {
        assertEquals(Contacts.Decision.New, Contacts.decide(listOf(alice), mapOf(alice to "alice"), bob, "bob"))
        assertEquals(listOf(alice, bob), Contacts.withContact(listOf(alice), bob))
        assertEquals(listOf(alice), Contacts.withContact(listOf(alice), alice))
    }

    @Test fun existingKeyIsReportedNotDuplicated() {
        assertEquals(Contacts.Decision.AlreadyContact, Contacts.decide(listOf(alice), emptyMap(), alice, "alice"))
    }

    @Test fun sameNameWithDifferentKeyIsFlaggedAndNeverReplaces() {
        val decision = Contacts.decide(listOf(alice), mapOf(alice to "alice"), impostor, "alice")
        assertEquals(Contacts.Decision.NameConflict(alice), decision)
        assertEquals(listOf(alice, impostor), Contacts.withContact(listOf(alice), impostor))
    }

    @Test fun staleNameOfARemovedContactIsNotAConflict() {
        assertEquals(Contacts.Decision.New, Contacts.decide(emptyList(), mapOf(alice to "alice"), impostor, "alice"))
    }

    @Test fun adviceSaysToCompareOutsidePathweft() {
        assertTrue(Contacts.codeAdvice().contains("never over Pathweft"))
    }
}
