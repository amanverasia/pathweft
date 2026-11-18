package org.estudely.pathweft

/**
 * Platform-independent rules for adding a contact from a card. A pinned key is never replaced:
 * a card whose name matches an existing contact but whose key differs is a different person.
 */
object Contacts {
    sealed class Decision {
        object New : Decision()
        object AlreadyContact : Decision()
        data class NameConflict(val existingKey: String) : Decision()
    }

    /** [contacts] are pinned public keys; [names] maps public key -> label. */
    fun decide(contacts: List<String>, names: Map<String, String>, key: String, name: String?): Decision {
        if (key in contacts) return Decision.AlreadyContact
        if (name != null) {
            names.entries.firstOrNull { it.value == name && it.key != key && it.key in contacts }
                ?.let { return Decision.NameConflict(it.key) }
        }
        return Decision.New
    }

    /** Contacts after adding [key]; existing keys are kept and never reordered. */
    fun withContact(contacts: List<String>, key: String): List<String> =
        if (key in contacts) contacts else contacts + key

    /** What to do with the verification code. */
    fun codeAdvice(): String =
        "Compare this code with the other person in person or by a voice call, never over Pathweft. " +
            "If any digit differs, do not add the contact."
}
