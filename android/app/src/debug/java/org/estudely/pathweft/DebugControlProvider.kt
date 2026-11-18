package org.estudely.pathweft

import android.content.ContentProvider
import android.content.ContentValues
import android.content.Intent
import android.database.Cursor
import android.net.Uri
import android.os.Binder
import android.os.Bundle
import android.os.Process
import android.util.Base64
import org.json.JSONObject
import java.util.concurrent.TimeUnit

/** Debug APK only. No network control listener; only the ADB shell/app UID may call. */
class DebugControlProvider : ContentProvider() {
    override fun onCreate(): Boolean { NodeController.initialize(requireNotNull(context)); return true }
    override fun call(method: String, arg: String?, extras: Bundle?): Bundle {
        check(BuildConfig.DEBUG)
        val caller = Binder.getCallingUid()
        if (caller != 2000 && caller != Process.myUid()) throw SecurityException("Debug control requires the ADB shell")
        require((arg?.length ?: 0) <= 180000) { "request exceeds size limit" }
        val payload = if (arg.isNullOrEmpty()) JSONObject() else
            JSONObject(String(Base64.decode(arg, Base64.DEFAULT), Charsets.UTF_8))
        val response = try {
            if (method == "start") requireNotNull(context).startForegroundService(
                Intent(context, PathweftService::class.java))
            if (method == "stop") {
                requireNotNull(context).stopService(Intent(context, PathweftService::class.java))
                val deadline = android.os.SystemClock.elapsedRealtime() + 3000
                while (PathweftService.active && android.os.SystemClock.elapsedRealtime() < deadline) Thread.sleep(20)
                check(!PathweftService.active) { "Foreground service did not stop" }
            }
            NodeController.submit(method, payload).get(15, TimeUnit.SECONDS)
        } catch (error: Exception) {
            JSONObject().put("ok", false).put("error", error.cause?.message ?: error.message ?: "Control failed")
        }
        return Bundle().apply { putString("result", Base64.encodeToString(response.toString().toByteArray(Charsets.UTF_8), Base64.NO_WRAP)) }
    }
    override fun query(uri: Uri, projection: Array<out String>?, selection: String?, selectionArgs: Array<out String>?, sortOrder: String?): Cursor? = throw SecurityException("unsupported")
    override fun getType(uri: Uri): String? = null
    override fun insert(uri: Uri, values: ContentValues?): Uri? = throw SecurityException("unsupported")
    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?): Int = throw SecurityException("unsupported")
    override fun update(uri: Uri, values: ContentValues?, selection: String?, selectionArgs: Array<out String>?): Int = throw SecurityException("unsupported")
}
