package org.estudely.pathweft

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.SystemClock
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.uiautomator.*
import org.json.JSONObject
import org.junit.*
import org.junit.Assert.*
import org.junit.runners.MethodSorters
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/** Run on a disposable emulator: exercises real buttons and the OS permission dialog. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ControlUiTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val device = UiDevice.getInstance(instrumentation)
    private val packageName = context.packageName

    private fun view(name: String): UiObject2 {
        val selector = By.res(packageName, name)
        if (!device.wait(Until.hasObject(selector), 2000) && device.hasObject(By.scrollable(true))) {
            UiScrollable(UiSelector().scrollable(true)).scrollIntoView(
                UiSelector().resourceId("$packageName:id/$name"))
        }
        val found = settled(selector, "view $name")
        // A long page can leave a control half-hidden under the status bar after an earlier scroll; the tap
        // would then land on the system bar and be lost. Controls at the top of the page are reached from the top.
        if (found.visibleBounds.top < 100 && device.hasObject(By.scrollable(true))) {
            scrollToTop()
            return settled(selector, "view $name")
        }
        return found
    }
    /** The result line sits at the bottom of the page, off-screen on a phone, and UiAutomator only sees on-screen nodes. */
    private fun resultLine(): BySelector {
        view("operation_result")
        return By.res(packageName, "operation_result")
    }
    /**
     * Find an object and wait until its position has been stable for 200 ms. Scrolling views keep gliding
     * after a scroll call returns, and dialogs/system prompts animate in; a tap during that motion is
     * swallowed (seen on the API 37 emulator and the Moto g52).
     */
    private fun settled(selector: BySelector, what: String): UiObject2 {
        var bounds = (device.wait(Until.findObject(selector), 5000) ?: error("Missing $what")).visibleBounds
        val deadline = SystemClock.elapsedRealtime() + 3000
        while (SystemClock.elapsedRealtime() < deadline) {
            SystemClock.sleep(200)
            val now = (device.findObject(selector) ?: break).visibleBounds
            if (now == bounds) break
            bounds = now
        }
        return device.findObject(selector) ?: error("Missing $what")
    }
    private fun await(description: String, condition: () -> Boolean) {
        val deadline = SystemClock.elapsedRealtime() + 5000
        while (!condition()) {
            check(SystemClock.elapsedRealtime() < deadline) { "Timed out: $description; ${NodeController.lastMessage}" }
            SystemClock.sleep(50)
        }
    }
    /** A ScrollView whose content fits the screen is not exposed as scrollable, so only scroll when it is. */
    private fun scrollToTop() {
        if (device.hasObject(By.scrollable(true))) UiScrollable(UiSelector().scrollable(true)).scrollToBeginning(10)
    }
    private fun assertRunning(running: Boolean) {
        await("running=$running") { NodeController.running == running && PathweftService.active == running }
        scrollToTop()
        assertTrue(device.wait(Until.hasObject(By.res(packageName, "network_status")
            .textStartsWith(if (running) "Running" else "Network stopped")), 5000))
    }
    /** Granting never restarts the app process (revoking would), so tests that only need a running network set it up. */
    private fun grantStartPermissions() {
        if (Build.VERSION.SDK_INT >= 33) instrumentation.uiAutomation.grantRuntimePermission(packageName, Manifest.permission.POST_NOTIFICATIONS)
        if (Build.VERSION.SDK_INT >= 37) instrumentation.uiAutomation.grantRuntimePermission(packageName, LOCAL_NETWORK)
    }
    private fun launch() {
        context.startActivity(Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
        assertTrue(device.wait(Until.hasObject(By.res(packageName, "start_network")), 5000))
    }
    @After fun stopTestService() {
        context.stopService(Intent(context, PathweftService::class.java))
        await("test service stopped") { !PathweftService.active && !NodeController.running }
    }

    /** Android 17+, fresh install only. Runs first: the other tests grant access. Refuses twice, then checks the Settings path. */
    @Test fun a0_localNetworkExplanationRefusalAndSettingsPath() {
        Assume.assumeTrue(Build.VERSION.SDK_INT >= 37)
        Assume.assumeTrue(context.checkSelfPermission(LOCAL_NETWORK) == PackageManager.PERMISSION_DENIED)
        launch()
        // Dialog buttons may be shown in capitals, so use the framework's stable button IDs. Wait for the
        // titled dialog to appear and to close, so a closing dialog's button is never tapped by mistake.
        fun answer(title: String, positive: Boolean) {
            view("start_network").click()
            assertTrue("Missing dialog: $title", device.wait(Until.hasObject(By.text(title)), 5000))
            settled(By.res("android", if (positive) "button1" else "button2"), "dialog button").click()
            assertTrue(device.wait(Until.gone(By.text(title)), 5000))
        }
        fun expectRefusalNotice() {
            assertTrue(device.wait(Until.hasObject(resultLine().textContains("needs local network access")), 5000))
            assertFalse(NodeController.running)
        }
        // "Not now" on our explanation: no system prompt, clear message.
        answer("Allow local connections", positive = false)
        expectRefusalNotice()
        // Continue, then refuse in Android's own prompt, twice.
        repeat(2) {
            answer("Allow local connections", positive = true)
            // The second prompt offers "Don't allow" with a different ID: deny and don't ask again.
            settled(By.res(Pattern.compile(
                "com\\.android\\.permissioncontroller:id/permission_deny(_and_dont_ask_again)?_button")),
                "Android's local network prompt").click()
            expectRefusalNotice()
        }
        // Android no longer asks: the app offers Settings instead.
        answer("Local connections are turned off", positive = true)
        assertTrue(device.wait(Until.hasObject(By.pkg("com.android.settings")), 5000))
        device.pressBack()
        instrumentation.uiAutomation.grantRuntimePermission(packageName, LOCAL_NETWORK)
        launch()
        assertTrue(device.wait(Until.gone(resultLine().textContains("needs local network access")), 5000))
    }

    @Test fun a_permissionDenialRelayPersistenceAndBackgroundLifecycle() {
        // This test deliberately requires a fresh install, rather than silently
        // granting permissions or resetting a real user's app state.
        Assume.assumeTrue(Build.VERSION.SDK_INT == 36)
        launch()
        await("identity") { NodeController.publicIdentity.optString("id").length == 64 }
        val identity = NodeController.publicIdentity.toString()
        assertTrue(view("relay_enabled").isChecked)
        assertEquals(PackageManager.PERMISSION_DENIED, context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS))
        view("start_network").click()
        settled(By.res("com.android.permissioncontroller", "permission_deny_button"),
            "notification permission dialog on a fresh install").click()
        assertRunning(true)
        assertEquals(PackageManager.PERMISSION_DENIED, context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS))
        view("relay_enabled").click()
        await("relay opt-out") { !NodeController.snapshot.optBoolean("relay", true) && !JSONObject(NodeController.settings()).getBoolean("relay") }
        device.pressHome()
        assertTrue(NodeController.running && PathweftService.active)
        launch()
        view("stop_network").click()
        assertRunning(false)
        assertFalse(File(context.filesDir, "pathweft/control.sock").exists())
        view("start_network").click()
        assertRunning(true)
        assertFalse(view("relay_enabled").isChecked)
        assertEquals(identity, NodeController.publicIdentity.toString())
        view("relay_enabled").click()
        await("relay opt-in") { NodeController.snapshot.optBoolean("relay", false) }
    }

    @Test fun b_invalidSettingsPreserveSavedConfigurationAndRunningListener() {
        grantStartPermissions()
        launch()
        val settings = NodeController.settings()
        fun applyInvalid() {
            view("connection_settings").click()
            view("settings_json").text = """{"name":"bad name"}"""
            settled(By.res("android", "button1"), "dialog Apply button").click()
            await("invalid settings error") { NodeController.lastMessage.contains("node name must") }
            assertEquals(settings, NodeController.settings())
        }
        applyInvalid()
        assertRunning(false)
        view("start_network").click()
        assertRunning(true)
        val listener = NodeController.snapshot.getString("listen")
        // Clear the previous error before checking the second application.
        NodeController.submit("relay", JSONObject().put("enabled", true)).get(5, TimeUnit.SECONDS)
        applyInvalid()
        assertRunning(true)
        assertEquals(listener, NodeController.snapshot.getString("listen"))
    }

    @Test fun c_buttonsKeepTheirPositionWhenTheNetworkStarts() {
        grantStartPermissions()
        launch()
        assertRunning(false)
        val stopped = view("start_network").visibleBounds
        view("start_network").click()
        assertRunning(true)
        assertEquals(stopped, view("start_network").visibleBounds)
        view("stop_network").click()
        assertRunning(false)
        assertEquals(stopped, view("start_network").visibleBounds)
    }

    @Test fun c1_findRoutesSwitchIsOffByDefaultAndSavesItsSetting() {
        grantStartPermissions()
        launch()
        assertRunning(false)
        fun saved() = JSONObject(NodeController.settings()).optBoolean("discovery", false)
        assertFalse(view("discovery_enabled").isChecked)
        assertFalse(saved())
        try {
            view("discovery_enabled").click()
            await("discovery saved on") { saved() }
            assertTrue(view("discovery_enabled").isChecked)
            // A running node restarts to apply it and then reports discovery in its status.
            view("start_network").click()
            assertRunning(true)
            await("discovery enabled in node status") {
                NodeController.snapshot.optJSONObject("discovery")?.optBoolean("enabled") == true }
            view("discovery_enabled").click()
            await("discovery saved off") { !saved() }
            assertRunning(true)
            await("discovery disabled in node status") {
                NodeController.snapshot.optJSONObject("discovery")?.optBoolean("enabled") == false }
            assertFalse(view("discovery_enabled").isChecked)
        } finally {
            // Leave the saved setting as found (off) so later tests, and a real install, are unaffected.
            if (saved()) NodeController.submit("configure", JSONObject(NodeController.settings()).put("discovery", false))
                .get(10, TimeUnit.SECONDS)
        }
    }

    @Test fun d_savedMessageControlsRequireNetworkAndOpenEmptyDialogs() {
        grantStartPermissions()
        launch()
        assertRunning(false)
        view("queue_message").click()
        assertFalse(NodeController.running)
        assertFalse(File(context.filesDir, "pathweft/control.sock.outbox.json").exists())
        view("saved_outgoing").click()
        assertFalse(device.hasObject(By.text("No saved outgoing messages.")))
        view("start_network").click()
        assertRunning(true)
        view("saved_outgoing").click()
        assertTrue(device.wait(Until.hasObject(By.text("No saved outgoing messages.")), 5000))
        device.pressBack()
        assertTrue(device.wait(Until.gone(By.text("No saved outgoing messages.")), 5000))
        view("saved_incoming").click()
        assertTrue(device.wait(Until.hasObject(By.text("No saved incoming messages.")), 5000))
        device.pressBack()
        assertTrue(device.wait(Until.gone(By.text("No saved incoming messages.")), 5000))
    }

    /**
     * App-filesystem durability, in two separate instrumentation runs (so two app processes):
     *   am instrument -e durability write  ...  queues a message that cannot be sent (no route);
     *   am instrument -e durability verify ...  in a new process, the same entry must still be queued.
     * Skipped unless the argument is given. Verify cancels/forgets the entry and restores settings.
     */
    @Test fun e_savedOutgoingSurvivesAppProcessRestart() {
        val phase = InstrumentationRegistry.getArguments().getString("durability")
        Assume.assumeTrue(phase == "write" || phase == "verify")
        grantStartPermissions()
        val marker = File(context.filesDir, "durability-test.json")
        if (phase == "write") {
            val original = NodeController.settings()
            val settings = JSONObject(original).put("contacts", org.json.JSONArray().put(TEST_CONTACT_KEY))
            assertTrue(NodeController.submit("configure", settings).get(10, TimeUnit.SECONDS).optBoolean("ok"))
            launch()
            view("start_network").click()
            assertRunning(true)
            val queued = NodeController.submit("queue", JSONObject().put("to", TEST_CONTACT_ID)
                .put("message", "durability check").put("ttl_ms", 3_600_000)).get(10, TimeUnit.SECONDS)
            assertTrue(queued.toString(), queued.optBoolean("ok"))
            val id = queued.getJSONObject("result").getString("message_id")
            marker.writeText(JSONObject().put("message_id", id).put("settings", original).toString())
        } else {
            val saved = JSONObject(marker.readText())
            val id = saved.getString("message_id")
            launch()
            view("start_network").click()
            assertRunning(true)
            val outbox = NodeController.submit("outbox").get(10, TimeUnit.SECONDS)
            assertTrue(outbox.toString(), outbox.optBoolean("ok"))
            val entries = outbox.getJSONObject("result").getJSONArray("entries")
            val entry = (0 until entries.length()).map { entries.getJSONObject(it) }.single { it.getString("message_id") == id }
            assertEquals("queued", entry.getString("state"))
            assertTrue(NodeController.submit("cancel", JSONObject().put("message_id", id)).get(10, TimeUnit.SECONDS).optBoolean("ok"))
            assertTrue(NodeController.submit("forget", JSONObject().put("message_id", id)).get(10, TimeUnit.SECONDS).optBoolean("ok"))
            assertTrue(NodeController.submit("configure", JSONObject(saved.getString("settings"))).get(10, TimeUnit.SECONDS).optBoolean("ok"))
            marker.delete()
        }
    }

    /** Show own QR card, then add a contact from a pasted card after seeing the verification code. */
    @Test fun f_contactCardQrAndAddFromPastedCard() {
        val settings = NodeController.settings()
        val names = context.getSharedPreferences("contacts", 0).getString("names", null)
        try {
            grantStartPermissions()
            launch()
            view("start_network").click()
            assertRunning(true)
            val listener = NodeController.snapshot.getString("listen")
            view("my_contact_card").click()
            assertTrue(device.wait(Until.hasObject(By.res(packageName, "contact_qr")), 5000))
            val card = settled(By.res(packageName, "contact_card_text"), "card text").text
            assertTrue(card, card.startsWith("pathweft:contact/1/"))
            settled(By.res("android", "button2"), "Close").click()
            assertTrue(device.wait(Until.gone(By.res(packageName, "contact_qr")), 5000))

            view("add_contact").click()
            settled(By.res(packageName, "contact_card_input"), "card input").text = TEST_CONTACT_CARD
            settled(By.res("android", "button1"), "Check").click()
            val code = settled(By.res(packageName, "contact_code"), "verification code").text
            assertTrue(code, Regex("""\d{5}( \d{5}){5}""").matches(code))
            settled(By.res("android", "button1"), "Add contact").click()
            val deadline = SystemClock.elapsedRealtime() + 5000
            while (!JSONObject(NodeController.settings()).getJSONArray("contacts").toString().contains(TEST_CONTACT_KEY)) {
                check(SystemClock.elapsedRealtime() < deadline) { "contact was not added" }
                SystemClock.sleep(50)
            }
            // Added live: the node kept running on the same listener (no restart).
            assertTrue(NodeController.running)
            assertEquals(listener, NodeController.snapshot.getString("listen"))
        } finally {
            NodeController.submit("configure", JSONObject(settings)).get(10, TimeUnit.SECONDS)
            context.getSharedPreferences("contacts", 0).edit().apply {
                if (names == null) remove("names") else putString("names", names)
            }.commit()
        }
    }

    /**
     * Add contact -> Scan opens the camera scanner, and Back returns without adding anything.
     * With -e scan_expect true (emulator started with the test card's QR as its virtual-scene poster),
     * waits for a real camera scan to reach the verification screen instead.
     */
    @Test fun g_scanOpensCameraAndReturns() {
        instrumentation.uiAutomation.grantRuntimePermission(packageName, Manifest.permission.CAMERA)
        val expectScan = InstrumentationRegistry.getArguments().getString("scan_expect") == "true"
        launch()
        view("add_contact").click()
        settled(By.res("android", "button3"), "Scan button").click()
        assertTrue("scanner did not open", device.wait(Until.hasObject(By.res(packageName, "scanner_hint")), 5000))
        if (expectScan) {
            val code = device.wait(Until.findObject(By.res(packageName, "contact_code")), 60_000)
                ?: error("no card scanned within 60 s")
            assertTrue(code.text, Regex("""\d{5}( \d{5}){5}""").matches(code.text))
            settled(By.res("android", "button2"), "Cancel").click()
        } else {
            device.pressBack()
            assertTrue(device.wait(Until.hasObject(By.res(packageName, "start_network")), 5000))
            assertFalse(device.hasObject(By.res(packageName, "contact_code")))
        }
    }

    private companion object {
        const val LOCAL_NETWORK = "android.permission.ACCESS_LOCAL_NETWORK"
        // A fixed test identity (public data only); nothing is ever sent to it because no route exists.
        const val TEST_CONTACT_KEY = "9bac8e2946084555d8867adddc9a9f6aa1d3f1ea6194eabd993b544f809dce29"
        const val TEST_CONTACT_ID = "3bcb146bab0108dad5d0d138bb586124924f774d2348751278901470a6513a8e"
        const val TEST_CONTACT_CARD = "pathweft:contact/1/9bac8e2946084555d8867adddc9a9f6aa1d3f1ea6194eabd993b544f809dce29/4892/tester"
    }
}
