package org.estudely.pathweft

import android.Manifest
import android.app.Activity
import android.app.AlertDialog
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Typeface
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import android.text.InputType
import android.view.View
import android.view.WindowInsets
import android.view.WindowManager
import android.widget.*
import org.json.JSONObject
import java.net.Inet4Address
import java.net.NetworkInterface

class MainActivity : Activity() {
    private val handler = Handler(Looper.getMainLooper())
    private lateinit var status: TextView
    private lateinit var identity: TextView
    private lateinit var result: TextView
    private lateinit var received: TextView
    private lateinit var relay: Switch
    private lateinit var discovery: Switch
    private lateinit var bluetooth: TextView
    private val savedDialogs by lazy { SavedMessageDialogs(this) }
    private val contactDialogs by lazy { ContactDialogs(this) { scanContact() } }
    private var updating = false
    /** Shown instead of the service's last message until the user acts again. */
    private var notice: String? = null
    private val poll = object : Runnable {
        override fun run() { render(); handler.postDelayed(this, 500) }
    }
    override fun onCreate(state: Bundle?) {
        super.onCreate(state)
        if (BuildConfig.DEBUG) window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        NodeController.initialize(this)
        val column = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(32, 24, 32, 40) }
        column.setOnApplyWindowInsetsListener { view, insets ->
            val top = if (Build.VERSION.SDK_INT >= 30) insets.getInsets(WindowInsets.Type.systemBars()).top else insets.systemWindowInsetTop
            val bottom = if (Build.VERSION.SDK_INT >= 30) insets.getInsets(WindowInsets.Type.systemBars()).bottom else insets.systemWindowInsetBottom
            view.setPadding(32, top + 24, 32, bottom + 40)
            insets
        }
        setContentView(ScrollView(this).apply { addView(column) })
        fun text(value: String, size: Float = 16f): TextView = TextView(this).apply {
            text = value; textSize = size; setPadding(0, 12, 0, 12); column.addView(this)
        }
        fun button(label: String, action: () -> Unit) = Button(this).apply {
            text = label; setOnClickListener { action() }; column.addView(this)
        }
        text("Pathweft", 28f)
        text("Every path. One network.\nAndroid LAN development app")
        // Running status uses up to four lines; reserve them so the buttons never move.
        status = text("Preparing identity…").apply { id = R.id.network_status; minLines = 4 }
        identity = text("").apply { id = R.id.public_identity; typeface = Typeface.MONOSPACE; textSize = 12f; setTextIsSelectable(true) }
        button("Copy my public key") {
            getSystemService(ClipboardManager::class.java).setPrimaryClip(
                ClipData.newPlainText("Pathweft public key", NodeController.publicIdentity.optString("public_key")))
            Toast.makeText(this, "Public key copied", Toast.LENGTH_SHORT).show()
        }
        button("Start network") { startNetwork() }.id = R.id.start_network
        button("Stop network") { stopService(Intent(this, PathweftService::class.java)) }.id = R.id.stop_network
        relay = Switch(this).apply { id = R.id.relay_enabled; text = "Relay messages for other devices"; isChecked = true; column.addView(this) }
        relay.setOnCheckedChangeListener { _, checked ->
            if (!updating) NodeController.submit("relay", JSONObject().put("enabled", checked))
        }
        discovery = Switch(this).apply { id = R.id.discovery_enabled; text = "Find routes automatically"; isChecked = false; column.addView(this) }
        discovery.setOnCheckedChangeListener { _, checked ->
            // Rare setting: applying it restarts a running node, like any other configuration change.
            if (!updating) NodeController.submit("configure", JSONObject(NodeController.settings()).put("discovery", checked))
        }
        text("Finds paths to your contacts through nearby Pathweft devices. Off by default.", 14f)
        bluetooth = text("Bluetooth stopped").apply { id = R.id.bluetooth_status }
        button("Listen over Bluetooth") { startBluetooth() }.id = R.id.start_bluetooth
        button("Stop Bluetooth") { NodeController.submit("ble_stop") }.id = R.id.stop_bluetooth
        button("Connection settings") {
            val input = EditText(this).apply { id = R.id.settings_json; setText(NodeController.settings()); minLines = 8; textSize = 12f;
                typeface = Typeface.MONOSPACE; inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_MULTI_LINE }
            AlertDialog.Builder(this).setTitle("Pinned contacts and manual routes").setView(input)
                .setMessage("Public keys identify endpoints. Routes select the next device. These are development settings.")
                .setNegativeButton("Cancel", null).setPositiveButton("Apply") { _, _ ->
                    try { NodeController.submit("configure", JSONObject(input.text.toString())) }
                    catch (_: Exception) { Toast.makeText(this, "Settings must be valid JSON", Toast.LENGTH_LONG).show() }
                }.show()
        }.id = R.id.connection_settings
        text("Send a test message", 20f)
        val destination = EditText(this).apply { id = R.id.recipient_id; hint = "Recipient node ID (64 hex characters)"; isSingleLine = true; column.addView(this) }
        val message = EditText(this).apply { id = R.id.message_text; hint = "Message (up to 512 UTF-8 bytes)"; minLines = 2;
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_MULTI_LINE; column.addView(this) }
        button("Send and confirm delivery") {
            if (!NodeController.running) Toast.makeText(this, "Start the network first", Toast.LENGTH_SHORT).show()
            else NodeController.submit("send", JSONObject().put("to", destination.text.toString().trim())
                .put("message", message.text.toString()).put("timeout_ms", 3000))
        }.id = R.id.send_message
        button("Save in outbox") {
            if (!NodeController.running) Toast.makeText(this, "Start the network first", Toast.LENGTH_SHORT).show()
            else NodeController.submit("queue", JSONObject().put("to", destination.text.toString().trim())
                .put("message", message.text.toString()).put("ttl_ms", 3_600_000))
        }.id = R.id.queue_message
        result = text("").apply { id = R.id.operation_result }
        button("Saved outgoing") { savedDialogs.showOutbox() }.id = R.id.saved_outgoing
        button("Saved incoming") { savedDialogs.showInbox() }.id = R.id.saved_incoming
        button("My contact card") { contactDialogs.showMyCard() }.id = R.id.my_contact_card
        button("Add contact") { contactDialogs.showAdd() }.id = R.id.add_contact
        text("Received messages", 20f)
        received = text("No messages yet.")
    }
    private fun startBluetooth() {
        if (!NodeController.running || !PathweftService.active) {
            Toast.makeText(this, "Start the network first", Toast.LENGTH_SHORT).show(); return
        }
        if (Build.VERSION.SDK_INT < 29) {
            Toast.makeText(this, "Bluetooth links need Android 10 or later", Toast.LENGTH_LONG).show(); return
        }
        if (Build.VERSION.SDK_INT >= 31) {
            val permissions = arrayOf(Manifest.permission.BLUETOOTH_CONNECT, Manifest.permission.BLUETOOTH_ADVERTISE)
                .filter { checkSelfPermission(it) != PackageManager.PERMISSION_GRANTED }
            if (permissions.isNotEmpty()) { requestPermissions(permissions.toTypedArray(), 42); return }
        }
        NodeController.submit("ble_start")
    }
    private fun startNetwork() {
        notice = null
        if (Build.VERSION.SDK_INT >= 37 && checkSelfPermission(LOCAL_NETWORK) != PackageManager.PERMISSION_GRANTED) {
            if (getPreferences(0).getBoolean("localNetworkAsked", false) && !shouldShowRequestPermissionRationale(LOCAL_NETWORK)) {
                showLocalNetworkSettings()
            } else {
                AlertDialog.Builder(this).setTitle("Allow local connections")
                    .setMessage("Pathweft sends and relays messages by connecting directly to other devices on the same Wi-Fi.\n\n" +
                        "Android calls this \"Nearby devices\" and mentions finding their relative position. " +
                        "Pathweft only uses it to connect to devices on your local network. It does not use your location.")
                    .setNegativeButton("Not now") { _, _ -> refused() }
                    .setPositiveButton("Continue") { _, _ ->
                        getPreferences(0).edit().putBoolean("localNetworkAsked", true).apply()
                        requestPermissions(arrayOf(LOCAL_NETWORK), 40)
                    }.show()
            }
            return
        }
        val preferences = getPreferences(0)
        if (Build.VERSION.SDK_INT >= 33 && !preferences.getBoolean("notificationsAsked", false)
            && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            preferences.edit().putBoolean("notificationsAsked", true).apply()
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 41); return
        }
        startForegroundService(Intent(this, PathweftService::class.java))
    }
    private fun refused() {
        notice = "Network not started: Pathweft needs local network access (\"Nearby devices\") to connect to other devices."
        render()
    }
    /** Android stops showing its prompt after repeated refusals; only Settings can grant it then. */
    private fun showLocalNetworkSettings() {
        AlertDialog.Builder(this).setTitle("Local connections are turned off")
            .setMessage("To start the network, open Settings → Permissions → Nearby devices and choose Allow.")
            .setNegativeButton("Not now") { _, _ -> refused() }
            .setPositiveButton("Open settings") { _, _ ->
                startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.fromParts("package", packageName, null)))
            }.show()
    }
    private fun scanContact() {
        if (checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
            @Suppress("DEPRECATION") startActivityForResult(Intent(this, ScannerActivity::class.java), SCAN_REQUEST)
        } else requestPermissions(arrayOf(Manifest.permission.CAMERA), CAMERA_REQUEST)
    }
    @Deprecated("Plain Activity result API; this app has no AndroidX dependency.")
    override fun onActivityResult(code: Int, resultCode: Int, data: Intent?) {
        @Suppress("DEPRECATION") super.onActivityResult(code, resultCode, data)
        val card = data?.getStringExtra(ScannerActivity.EXTRA_CARD)
        if (code == SCAN_REQUEST && resultCode == RESULT_OK && card != null) contactDialogs.check(card)
    }
    override fun onRequestPermissionsResult(code: Int, permissions: Array<out String>, results: IntArray) {
        super.onRequestPermissionsResult(code, permissions, results)
        when {
            code == CAMERA_REQUEST && results.firstOrNull() == PackageManager.PERMISSION_GRANTED -> scanContact()
            code == CAMERA_REQUEST && results.isNotEmpty() ->
                Toast.makeText(this, "Camera access is needed only to scan a contact's QR code. You can paste their card instead.", Toast.LENGTH_LONG).show()
            code == 42 && results.isNotEmpty() && results.all { it == PackageManager.PERMISSION_GRANTED } -> startBluetooth()
            code == 42 -> { notice = "Bluetooth not started: allow Nearby devices in app permissions."; render() }
            code == 41 || (code == 40 && results.firstOrNull() == PackageManager.PERMISSION_GRANTED) -> startNetwork()
            // An empty result means the prompt was interrupted, not refused.
            code == 40 && results.isNotEmpty() -> refused()
        }
    }
    private fun wifiAddress(): String = try {
        NetworkInterface.getNetworkInterfaces().toList().filter { it.name == "wlan0" }
            .flatMap { it.inetAddresses.toList() }.filterIsInstance<Inet4Address>().firstOrNull()?.hostAddress ?: "not connected"
    } catch (_: Exception) { "unavailable" }
    private fun render() {
        NodeController.poll()
        val snapshot = NodeController.snapshot
        val listener = snapshot.optString("listen", "")
        val found = snapshot.optJSONObject("discovery")?.takeIf { it.optBoolean("enabled") }
            ?.let { "\nRoutes found automatically: ${it.optInt("routes")}" } ?: ""
        status.show(if (NodeController.running) "Running · Wi-Fi ${wifiAddress()}\nTCP $listener\nForwarded: ${snapshot.optInt("forwarded")} · Received: ${snapshot.optInt("delivered_texts")}$found" else "Network stopped · Wi-Fi ${wifiAddress()}")
        identity.show("Node ID\n${NodeController.publicIdentity.optString("id")}\n\nPublic key\n${NodeController.publicIdentity.optString("public_key")}")
        updating = true
        relay.isChecked = if (NodeController.running) snapshot.optBoolean("relay", true) else JSONObject(NodeController.settings()).optBoolean("relay", true)
        discovery.isChecked = JSONObject(NodeController.settings()).optBoolean("discovery", false)
        updating = false
        result.show(notice ?: NodeController.lastMessage)
        val ble = snapshot.optJSONObject("bluetooth") ?: JSONObject()
        val links = ble.optJSONArray("links") ?: org.json.JSONArray()
        bluetooth.show(ble.optString("error").takeIf { it != "null" && it.isNotEmpty() }
            ?: if (ble.optBoolean("listening")) "Bluetooth ${if (ble.optBoolean("advertising")) "listening" else "starting"} · ${links.length()} links\n" +
                (0 until links.length()).joinToString("\n") { links.getJSONObject(it).optString("local", "Connecting…") }
            else "Bluetooth stopped")
        val events = NodeController.events()
        val messages = (0 until events.length()).map { events.getJSONObject(it) }.filter { it.optString("event") == "text_received" }
        received.show(messages.takeLast(12).joinToString("\n\n") {
            val details = it.getJSONObject("details")
            "${details.getString("source").take(12)}…\n${details.getString("text")}"
        }.ifEmpty { "No messages yet." })
    }
    /** Polling runs every 500 ms; only touch views whose text changed, so selections and UI inspection are not disrupted. */
    private fun TextView.show(value: String) { if (text.toString() != value) text = value }
    private companion object {
        const val LOCAL_NETWORK = "android.permission.ACCESS_LOCAL_NETWORK"
        const val CAMERA_REQUEST = 43
        const val SCAN_REQUEST = 50
    }
    override fun onResume() {
        super.onResume()
        // Returning from Settings with access granted: drop the stale refusal message.
        if (Build.VERSION.SDK_INT >= 37 && checkSelfPermission(LOCAL_NETWORK) == PackageManager.PERMISSION_GRANTED) notice = null
        handler.post(poll)
    }
    override fun onPause() { handler.removeCallbacks(poll); super.onPause() }
}
