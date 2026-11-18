package org.estudely.pathweft

import android.app.Activity
import android.app.AlertDialog
import android.content.ClipData
import android.content.ClipboardManager
import android.graphics.Bitmap
import android.graphics.Color
import android.graphics.Typeface
import android.widget.EditText
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject

/** Share your contact card as a QR code, and add a contact from someone else's card. */
class ContactDialogs(private val activity: Activity, private val scan: () -> Unit) {
    private fun request(operation: String, payload: JSONObject, action: (JSONObject) -> Unit) {
        NodeController.submit(operation, payload) { response ->
            activity.runOnUiThread {
                if (response.optBoolean("ok")) action(response.getJSONObject("result"))
                else Toast.makeText(activity, response.optString("error", "Operation failed"), Toast.LENGTH_LONG).show()
            }
        }
    }

    private fun column(): LinearLayout = LinearLayout(activity).apply {
        orientation = LinearLayout.VERTICAL
        setPadding(48, 24, 48, 24)
    }

    private fun text(column: LinearLayout, value: String, mono: Boolean = false) = TextView(activity).apply {
        text = value
        setPadding(0, 12, 0, 12)
        setTextIsSelectable(true)
        if (mono) typeface = Typeface.MONOSPACE
        column.addView(this)
    }

    fun showMyCard() = request("contact_card", JSONObject()) { result ->
        val card = result.getString("card")
        val column = column()
        text(column, "Let the other person scan this in person. Scanning in person is what makes it trustworthy.")
        column.addView(ImageView(activity).apply {
            id = R.id.contact_qr
            setImageBitmap(qrBitmap(result.getJSONArray("qr")))
            adjustViewBounds = true
            contentDescription = "Your contact QR code"
        })
        text(column, card, mono = true).id = R.id.contact_card_text
        AlertDialog.Builder(activity).setTitle("My contact card")
            .setView(ScrollView(activity).apply { addView(column) })
            .setNegativeButton("Close", null)
            .setPositiveButton("Copy card") { _, _ ->
                activity.getSystemService(ClipboardManager::class.java)
                    .setPrimaryClip(ClipData.newPlainText("Pathweft contact card", card))
                Toast.makeText(activity, "Contact card copied", Toast.LENGTH_SHORT).show()
            }.show()
    }

    fun showAdd() {
        val input = EditText(activity).apply {
            id = R.id.contact_card_input
            hint = "pathweft:contact/1/…"
            typeface = Typeface.MONOSPACE
            textSize = 12f
        }
        AlertDialog.Builder(activity).setTitle("Add contact")
            .setMessage("Scan the other person's QR code in person, or paste their contact card.")
            .setView(input)
            .setNegativeButton("Cancel", null)
            .setNeutralButton("Scan") { _, _ -> scan() }
            .setPositiveButton("Check") { _, _ -> check(input.text.toString().trim()) }.show()
    }

    /** Check a pasted or scanned card with the strict Rust parser, then show the confirmation. */
    fun check(card: String) = request("contact_parse", JSONObject().put("peer", card)) { confirm(it) }

    private fun confirm(peer: JSONObject) {
        val key = peer.getString("public_key")
        val name = peer.optString("name").takeIf { it.isNotEmpty() && !peer.isNull("name") }
        val settings = JSONObject(NodeController.settings())
        val contacts = settings.optJSONArray("contacts").let { a -> (0 until (a?.length() ?: 0)).map { a!!.getString(it) } }
        val names = savedNames()
        val decision = Contacts.decide(contacts, names, key, name)
        val column = column()
        text(column, "${name ?: "Unnamed"} · ${SavedMessages.shortId(peer.getString("id"))}")
        text(column, "Verification code").setTypeface(null, Typeface.BOLD)
        text(column, peer.getString("code"), mono = true).id = R.id.contact_code
        text(column, Contacts.codeAdvice())
        when (decision) {
            is Contacts.Decision.AlreadyContact -> text(column, "This key is already one of your contacts.")
            is Contacts.Decision.NameConflict -> text(column,
                "Warning: you already have a contact called \"$name\" with a different key " +
                    "(${SavedMessages.shortId(decision.existingKey)}). This is a different device or person; " +
                    "your existing contact will not be changed.")
            Contacts.Decision.New -> {}
        }
        text(column, "They must also add your card, or their replies will be dropped.")
        val dialog = AlertDialog.Builder(activity).setTitle("Check before adding")
            .setView(ScrollView(activity).apply { addView(column) })
            .setNegativeButton("Cancel", null)
        if (decision != Contacts.Decision.AlreadyContact) dialog.setPositiveButton("Add contact") { _, _ ->
            NodeController.submit("add_contact", JSONObject().put("public_key", key)) { response ->
                activity.runOnUiThread {
                    if (response.optBoolean("ok")) {
                        name?.let { saveName(key, it) }
                        Toast.makeText(activity, "Contact added", Toast.LENGTH_SHORT).show()
                    } else Toast.makeText(activity, response.optString("error", "Could not add contact"), Toast.LENGTH_LONG).show()
                }
            }
        }
        dialog.show()
    }

    private fun savedNames(): Map<String, String> {
        val json = JSONObject(activity.getSharedPreferences("contacts", 0).getString("names", "{}")!!)
        return json.keys().asSequence().associateWith { json.getString(it) }
    }

    private fun saveName(key: String, name: String) {
        val prefs = activity.getSharedPreferences("contacts", 0)
        val json = JSONObject(prefs.getString("names", "{}")!!).put(key, name)
        prefs.edit().putString("names", json.toString()).apply()
    }

    private fun qrBitmap(rows: JSONArray): Bitmap {
        val size = rows.length()
        val scale = 12
        val pixels = IntArray(size * size * scale * scale)
        for (y in 0 until size * scale) for (x in 0 until size * scale) {
            val dark = rows.getString(y / scale)[x / scale] == '1'
            pixels[y * size * scale + x] = if (dark) Color.BLACK else Color.WHITE
        }
        return Bitmap.createBitmap(pixels, size * scale, size * scale, Bitmap.Config.ARGB_8888)
    }
}
