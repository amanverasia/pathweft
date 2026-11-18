package org.estudely.pathweft

import android.app.Activity
import android.app.AlertDialog
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject

/** On-demand saved-message UI; no durable-message polling is added to the main screen. */
class SavedMessageDialogs(private val activity: Activity) {
    private var savedDialog: AlertDialog? = null

    fun showOutbox() = request("outbox") { showOutgoing(it) }

    fun showInbox() = request("inbox") { showIncoming(it) }

    private fun running(): Boolean {
        if (NodeController.running) return true
        Toast.makeText(activity, "Start the network first", Toast.LENGTH_SHORT).show()
        return false
    }

    private fun request(operation: String, messageId: String? = null, action: (JSONObject) -> Unit) {
        if (!running()) return
        val payload = JSONObject()
        messageId?.let { payload.put("message_id", it) }
        NodeController.submit(operation, payload) { response ->
            activity.runOnUiThread {
                if (response.optBoolean("ok")) action(response.getJSONObject("result"))
                else Toast.makeText(activity, response.optString("error", "Operation failed"), Toast.LENGTH_LONG).show()
            }
        }
    }

    private fun column(): LinearLayout = LinearLayout(activity).apply {
        orientation = LinearLayout.VERTICAL
        setPadding(32, 16, 32, 16)
    }

    private fun text(column: LinearLayout, value: String) = TextView(activity).apply {
        text = value
        setPadding(0, 16, 0, 8)
        column.addView(this)
    }

    private fun button(column: LinearLayout, label: String, action: () -> Unit) = Button(activity).apply {
        text = label
        setOnClickListener { action() }
        column.addView(this)
    }

    private fun showSaved(title: String, column: LinearLayout) {
        savedDialog?.dismiss()
        savedDialog = AlertDialog.Builder(activity).setTitle(title)
            .setView(ScrollView(activity).apply { addView(column) })
            .setNegativeButton("Close", null).show()
    }

    private fun JSONObject.recoveryError(): String? = listOf(
        "last_error", "last_query_error", "last_redelivery_error",
    ).map { optString(it, "") }
        .filter { it.isNotBlank() && it != "null" }
        .joinToString(", ")
        .takeIf { it.isNotEmpty() }

    private fun showOutgoing(result: JSONObject) {
        val column = column()
        val mutable = !result.optBoolean("storage_error")
        if (!mutable) text(column, "Storage error. Restart Pathweft before changing saved messages.")
        val entries = result.optJSONArray("entries") ?: JSONArray()
        if (entries.length() == 0) text(column, "No saved outgoing messages.")
        for (index in 0 until entries.length()) {
            val json = entries.getJSONObject(index)
            val entry = OutgoingEntry(
                id = json.getString("message_id"),
                recipient = json.getString("recipient"),
                state = json.getString("state"),
                attempts = json.optInt("attempts"),
                receiptQueries = json.optInt("receipt_queries"),
                redeliveries = json.optInt("redeliveries"),
                lastError = json.recoveryError(),
            )
            text(column, "${entry.headline()}\n${entry.details()}")
            if (mutable && entry.canCancel) {
                button(column, "Cancel queued message") {
                    request("cancel", entry.id) { showOutbox() }
                }
            }
            if (mutable && entry.canForget) {
                button(column, "Forget local history") {
                    confirmForget(
                        "Forget this outgoing entry? Recovery stops and local history is removed. This does not delete text at the recipient.",
                    ) { request("forget", entry.id) { showOutbox() } }
                }
            }
        }
        showSaved("Saved outgoing", column)
    }

    private fun showIncoming(result: JSONObject) {
        val column = column()
        val mutable = !result.optBoolean("storage_error")
        if (!mutable) text(column, "Storage error. Restart Pathweft before reading or changing saved messages.")
        val entries = result.optJSONArray("entries") ?: JSONArray()
        if (entries.length() == 0) text(column, "No saved incoming messages.")
        for (index in 0 until entries.length()) {
            val json = entries.getJSONObject(index)
            val entry = IncomingEntry(
                id = json.getString("message_id"),
                source = json.getString("source"),
            )
            text(column, entry.headline())
            if (mutable) {
                button(column, "Read message") {
                    request("inbox_read", entry.id) { showIncomingText(it) }
                }
                button(column, "Forget message") {
                    confirmForget(
                        "Forget this incoming message? Its local text is removed and Pathweft stops sending receipts for it.",
                    ) { request("inbox_forget", entry.id) { showInbox() } }
                }
            }
        }
        showSaved("Saved incoming", column)
    }

    private fun showIncomingText(result: JSONObject) {
        val content = ScrollView(activity).apply {
            addView(TextView(activity).apply {
                text = result.getString("text")
                setPadding(32, 16, 32, 16)
                setTextIsSelectable(true)
            })
        }
        AlertDialog.Builder(activity)
            .setTitle("From ${SavedMessages.shortId(result.optString("source"))}")
            .setView(content).setNegativeButton("Close", null).show()
    }

    private fun confirmForget(message: String, action: () -> Unit) {
        AlertDialog.Builder(activity).setTitle("Forget saved message?").setMessage(message)
            .setNegativeButton("Keep", null)
            .setPositiveButton("Forget") { _, _ -> action() }.show()
    }
}
