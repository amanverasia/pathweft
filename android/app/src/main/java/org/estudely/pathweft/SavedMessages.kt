package org.estudely.pathweft

/** Platform-independent presentation and mutation rules for saved-message controls. */
data class OutgoingEntry(
    val id: String,
    val recipient: String,
    val state: String,
    val attempts: Int,
    val receiptQueries: Int,
    val redeliveries: Int,
    val lastError: String?,
) {
    val canCancel: Boolean get() = state == "queued"
    val canForget: Boolean get() = state in terminalStates

    fun headline(): String = "${SavedMessages.stateLabel(state)} · ${SavedMessages.shortId(id)}"

    fun details(): String = buildString {
        append("To ${SavedMessages.shortId(recipient)}")
        append("\nAttempts $attempts · Queries $receiptQueries · Redeliveries $redeliveries")
        lastError?.takeIf { it.isNotBlank() }?.let {
            append("\nLast error: ${it.replace('_', ' ')}")
        }
    }

    private companion object {
        val terminalStates = setOf("delivered", "unconfirmed", "expired", "cancelled")
    }
}

data class IncomingEntry(val id: String, val source: String) {
    fun headline(): String = "From ${SavedMessages.shortId(source)}\nMessage ${SavedMessages.shortId(id)}"
}

object SavedMessages {
    fun shortId(value: String): String = if (value.length > 12) "${value.take(12)}…" else value

    fun stateLabel(state: String): String = state.replace('_', ' ')
        .replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
}
