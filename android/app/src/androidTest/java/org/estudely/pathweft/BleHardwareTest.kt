package org.estudely.pathweft

import android.content.Intent
import android.os.Build
import android.os.SystemClock
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.uiautomator.*
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert.*
import org.junit.Test
import java.net.Socket
import java.util.concurrent.TimeUnit

/** Opt-in hardware test. Run only with scripts/android_ble_demo.py's office coordinator. */
class BleHardwareTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val device = UiDevice.getInstance(instrumentation)
    private fun await(description: String, seconds: Int = 15, condition: () -> Boolean) {
        val deadline = SystemClock.elapsedRealtime() + seconds * 1000
        while (!condition()) {
            check(SystemClock.elapsedRealtime() < deadline) { "$description timed out: ${NodeController.snapshot}; ${NodeController.lastMessage}" }
            SystemClock.sleep(100)
        }
    }
    private fun request(operation: String, payload: JSONObject = JSONObject()): JSONObject {
        val response = NodeController.submit(operation, payload).get(15, TimeUnit.SECONDS)
        check(response.optBoolean("ok")) { response.toString() }
        return response.getJSONObject("result")
    }
    private fun click(name: String) {
        val selector = By.res(context.packageName, name)
        if (!device.hasObject(selector) && device.hasObject(By.scrollable(true))) {
            UiScrollable(UiSelector().scrollable(true)).scrollIntoView(UiSelector().resourceId("${context.packageName}:id/$name"))
        }
        checkNotNull(device.wait(Until.findObject(selector), 5000)) { "Missing $name" }.click()
    }
    private fun allowPrompt() {
        device.findObject(By.res("com.android.permissioncontroller", "permission_allow_button"))?.click()
        device.findObject(By.res("com.android.permissioncontroller", "permission_allow_foreground_only_button"))?.click()
        device.findObject(By.res("android", "button1").text("Continue"))?.click()
    }
    private fun received(text: String): Boolean {
        val events = NodeController.events()
        return (0 until events.length()).any {
            val event = events.getJSONObject(it)
            event.optString("event") == "text_received" && event.getJSONObject("details").optString("text") == text
        }
    }

    @Test fun encryptedTextAndReceiptsAcrossTheRealBluetoothLink() {
        val args = InstrumentationRegistry.getArguments()
        org.junit.Assume.assumeTrue("Use the dedicated BLE hardware coordinator", args.getString("bleHardware") == "true")
        check(Build.VERSION.SDK_INT >= 29)
        val key = checkNotNull(args.getString("peerKey"))
        val id = checkNotNull(args.getString("peerId"))
        check(key.matches(Regex("[0-9a-f]{64}")) && id.matches(Regex("[0-9a-f]{64}")))
        NodeController.initialize(context)
        check(!PathweftService.active && !NodeController.running) { "Stop Pathweft before the hardware test" }
        val original = JSONObject(NodeController.settings())
        val publicIdentity = request("identity").toString()
        try {
            request("configure", JSONObject().put("name", "BLEPhone").put("listen", "127.0.0.1:0")
                .put("relay", true).put("contacts", JSONArray().put(key)))
            context.startActivity(Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
            assertTrue(device.wait(Until.hasObject(By.res(context.packageName, "start_network")), 5000))
            click("start_network")
            await("network start") { allowPrompt(); NodeController.running }
            click("start_bluetooth")
            await("Bluetooth advertising") {
                allowPrompt()
                NodeController.snapshot.optJSONObject("bluetooth")?.optBoolean("advertising") == true
            }
            await("office Linux connection", 90) {
                NodeController.snapshot.optJSONObject("bluetooth")?.getJSONArray("links")?.length() == 1
            }
            val local = NodeController.snapshot.getJSONObject("bluetooth").getJSONArray("links").getJSONObject(0).getString("local")
            request("route", JSONObject().put("to", id).put("via", local))
            request("send", JSONObject().put("to", id).put("message", "office Android BLE 🌍").put("timeout_ms", 10000))
            await("Linux return text", 60) { received("office Linux BLE 🌍") }
            request("send", JSONObject().put("to", id).put("message", "office Android BLE final").put("timeout_ms", 10000))
            // The coordinator stops only Bluetooth after it has authenticated
            // both receipts. Keep the Rust node alive for the negative check.
            await("coordinator link stop", 30) { NodeController.snapshot.getJSONObject("bluetooth").optBoolean("listening") == false }
            assertThrows(java.io.IOException::class.java) {
                Socket("127.0.0.1", local.substringAfterLast(':').toInt()).close()
            }
            val failed = NodeController.submit("send", JSONObject().put("to", id).put("message", "must not deliver")
                .put("timeout_ms", 1000)).get(5, TimeUnit.SECONDS)
            assertFalse("Stopped Bluetooth must prevent delivery", failed.getBoolean("ok"))
            assertEquals(publicIdentity, request("identity").toString())
        } finally {
            context.stopService(Intent(context, PathweftService::class.java))
            await("service cleanup") { !PathweftService.active }
            request("stop")
            request("configure", original)
            device.pressHome()
        }
    }
}
