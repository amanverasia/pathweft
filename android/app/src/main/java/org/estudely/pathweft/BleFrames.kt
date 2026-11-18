package org.estudely.pathweft

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream

/** Same unparsed u32-BE length + body used by the Rust node and BLE adapter. */
internal object BleFrames {
    const val MAX_BYTES = 32768
    fun read(input: InputStream): ByteArray? {
        val first = input.read()
        if (first == -1) return null
        val data = DataInputStream(input)
        val length = (first shl 24) or (data.readUnsignedByte() shl 16) or
            (data.readUnsignedByte() shl 8) or data.readUnsignedByte()
        require(length in 1..MAX_BYTES) { "Bluetooth frame length outside 1..32768" }
        return ByteArray(length).also { data.readFully(it) }
    }
    fun write(output: OutputStream, body: ByteArray) {
        require(body.size in 1..MAX_BYTES) { "Bluetooth frame length outside 1..32768" }
        DataOutputStream(output).apply { writeInt(body.size); write(body); flush() }
    }
}
