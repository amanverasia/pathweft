package org.estudely.pathweft

import android.content.Context
import android.app.Application
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.util.ArrayDeque
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Serializes JNI calls off the UI thread; all networking runs in Rust. */
object NodeController {
    private lateinit var app: Application
    private lateinit var directory: String
    private lateinit var bluetooth: BleTransport
    private val worker = ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        ArrayBlockingQueue(16), ThreadPoolExecutor.AbortPolicy())
    private val polling = AtomicBoolean(false)
    /** Link reports must not wait behind sends, and must not run on Bluetooth threads. */
    private val linkWorker = java.util.concurrent.Executors.newSingleThreadExecutor { task ->
        Thread(task, "pathweft-live-links").apply { isDaemon = true } }
    private val liveLinks = LiveLinks { request ->
        try { linkWorker.execute { native("request", JSONObject().put("command", request.command)
            .put("address", request.address).also { json -> request.kind?.let { json.put("kind", it) } }) } }
        catch (_: java.util.concurrent.RejectedExecutionException) { }
    }
    private var nativeFailure: String? = null
    private val history = ArrayDeque<JSONObject>()
    @Volatile var running = false
        private set
    @Volatile var snapshot = JSONObject().put("running", false)
        private set
    @Volatile var publicIdentity = JSONObject()
        private set
    @Volatile var lastMessage = "Ready. Connect devices to the same local network."
        private set

    @Synchronized fun initialize(context: Context) {
        if (::app.isInitialized) return
        app = context.applicationContext as Application
        directory = File(app.filesDir, "pathweft").absolutePath
        bluetooth = BleTransport(app, liveLinks)
        worker.submit {
            val response = native("identity")
            if (response.optBoolean("ok")) publicIdentity = response.getJSONObject("result")
        }
    }

    fun settings(): String = app.getSharedPreferences("node", 0).getString("settings",
        """{"name":"Android","listen":"0.0.0.0:0","relay":true,"contacts":[],"routes":[]}""")!!

    private fun native(operation: String, payload: JSONObject = JSONObject()): JSONObject {
        nativeFailure?.let { return JSONObject().put("ok", false).put("error", it) }
        return try { JSONObject(NativeBridge.call(directory, operation, payload.toString())) }
        catch (_: LinkageError) {
            nativeFailure = "The native library could not be loaded. Install a corrected build."
            lastMessage = nativeFailure!!
            JSONObject().put("ok", false).put("error", nativeFailure)
        }
    }

    private fun identitySnapshot() = JSONObject(publicIdentity.toString()).put("running", false)

    private fun collectEvents(): JSONArray {
        val response = native("events")
        if (response.optBoolean("ok")) {
            val events = response.getJSONObject("result").getJSONArray("events")
            synchronized(history) {
                for (i in 0 until events.length()) {
                    if (history.size == 64) history.removeFirst()
                    history.addLast(events.getJSONObject(i))
                }
            }
        }
        return events()
    }

    fun events(): JSONArray = synchronized(history) { JSONArray(history.map { JSONObject(it.toString()) }) }

    private fun refresh() {
        snapshot = if (running) {
            val response = native("request", JSONObject().put("command", "status"))
            if (response.optBoolean("ok")) response.getJSONObject("result").put("running", true)
            else identitySnapshot().put("error", response.optString("error"))
        } else identitySnapshot()
        snapshot.put("bluetooth", bluetooth.status())
        collectEvents()
    }

    fun poll() {
        if (worker.activeCount != 0 || worker.queue.isNotEmpty() || !polling.compareAndSet(false, true)) return
        try {
            worker.submit { try { refresh() } finally { polling.set(false) } }
        } catch (_: java.util.concurrent.RejectedExecutionException) { polling.set(false) }
    }

    private fun updateSettings(settings: JSONObject) {
        app.getSharedPreferences("node", 0).edit().putString("settings", settings.toString()).apply()
    }

    fun submit(operation: String, payload: JSONObject = JSONObject(), completion: ((JSONObject) -> Unit)? = null): Future<JSONObject> = worker.submit<JSONObject> {
        val outcome = try {
            val response = when (operation) {
                "identity" -> native("identity")
                "start" -> if (running) JSONObject().put("ok", true).put("result", snapshot)
                    else native("start", JSONObject(settings())).also {
                        running = it.optBoolean("ok")
                        if (running) synchronized(history) { history.clear() }
                    }
                "stop" -> {
                    liveLinks.setEnabled(false)
                    bluetooth.stop()
                    native("stop").also { if (it.optBoolean("ok")) running = false }
                }
                "settings" -> JSONObject().put("ok", true).put("result", JSONObject(settings()))
                "status" -> { refresh(); JSONObject().put("ok", true).put("result", snapshot) }
                "events" -> JSONObject().put("ok", true).put("result", JSONObject().put("events", collectEvents()))
                "ble_start" -> {
                    check(running) { "Start the network first" }
                    val port = snapshot.getString("listen").substringAfterLast(':').toInt()
                    JSONObject().put("ok", true).put("result", bluetooth.listen(port))
                }
                "ble_stop" -> { bluetooth.stop(); JSONObject().put("ok", true).put("result", bluetooth.status()) }
                "configure" -> {
                    val validation = native("validate", payload)
                    if (!validation.optBoolean("ok")) return@submit validation.also {
                        lastMessage = it.optString("error", "Invalid settings")
                        completion?.invoke(it)
                    }
                    val old = JSONObject(settings())
                    val wasRunning = running
                    if (wasRunning) { liveLinks.setEnabled(false); bluetooth.stop(); native("stop"); running = false }
                    val result = if (wasRunning) native("start", payload) else
                        JSONObject().put("ok", true).put("result", JSONObject().put("saved", true))
                    if (result.optBoolean("ok")) {
                        updateSettings(payload); running = wasRunning
                        if (running) synchronized(history) { history.clear() }
                    }
                    else if (wasRunning) { running = native("start", old).optBoolean("ok") }
                    result
                }
                "relay" -> {
                    val enabled = payload.getBoolean("enabled")
                    val result = if (running) native("request", JSONObject().put("command", "relay").put("enabled", enabled))
                        else JSONObject().put("ok", true).put("result", JSONObject().put("relay", enabled))
                    if (result.optBoolean("ok")) updateSettings(JSONObject(settings()).put("relay", enabled))
                    result
                }
                "contact_card", "contact_parse", "verification_code" -> native(operation, payload)
                "add_contact" -> {
                    // Pin the key live when running (no restart, Bluetooth links stay up) and keep it in settings.
                    val key = payload.getString("public_key")
                    val settings = JSONObject(settings())
                    val contacts = settings.optJSONArray("contacts") ?: org.json.JSONArray()
                    val known = (0 until contacts.length()).any { contacts.getString(it) == key }
                    val updated = JSONObject(settings.toString()).put("contacts",
                        if (known) contacts else org.json.JSONArray(contacts.toString()).put(key))
                    val validation = native("validate", updated)
                    if (!validation.optBoolean("ok")) validation
                    else {
                        val live = if (running) native("request", JSONObject().put("command", "add_contact").put("public_key", key))
                            else JSONObject().put("ok", true).put("result", JSONObject().put("added", !known))
                        if (live.optBoolean("ok")) updateSettings(updated)
                        live
                    }
                }
                "send", "queue", "route", "outbox", "inbox", "cancel", "forget",
                "inbox_read", "inbox_forget" ->
                    native("request", JSONObject(payload.toString()).put("command", operation))
                else -> JSONObject().put("ok", false).put("error", "Unknown operation")
            }
            liveLinks.setEnabled(running && JSONObject(settings()).optBoolean("discovery"))
            if (operation !in setOf("events", "status", "identity", "outbox", "inbox", "inbox_read",
                    "contact_card", "contact_parse", "verification_code")) {
                lastMessage = if (response.optBoolean("ok")) {
                    when (operation) {
                        "send" -> "Delivered: recipient authenticated the message."
                        "queue" -> "Saved in outbox; delivery will retry in the background."
                        "add_contact" -> "Contact added."
                        else -> "$operation complete"
                    }
                } else response.optString("error", "Operation failed")
            }
            refresh()
            response
        } catch (error: Exception) {
            lastMessage = error.message ?: "Operation failed"
            JSONObject().put("ok", false).put("error", lastMessage)
        }
        completion?.invoke(outcome)
        outcome
    }
}
