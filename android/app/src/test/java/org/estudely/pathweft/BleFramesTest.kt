package org.estudely.pathweft

import org.junit.Assert.*
import org.junit.Test
import java.io.*

class BleFramesTest {
    @Test fun fragmentedAndConsecutiveFramesRoundTrip() {
        val body = ByteArray(BleFrames.MAX_BYTES) { it.toByte() }
        val wire = ByteArrayOutputStream()
        BleFrames.write(wire, body)
        BleFrames.write(wire, "hello 🌍".toByteArray())
        val fragments = object : ByteArrayInputStream(wire.toByteArray()) {
            override fun read(b: ByteArray, off: Int, len: Int) = super.read(b, off, minOf(len, 3))
        }
        assertArrayEquals(body, BleFrames.read(fragments))
        assertEquals("hello 🌍", String(BleFrames.read(fragments)!!, Charsets.UTF_8))
        assertNull(BleFrames.read(fragments))
    }
    @Test fun invalidLengthsAreRejectedBeforeReadingTheBody() {
        for (length in listOf(0, 32769, -1)) {
            val prefix = ByteArrayOutputStream().also { DataOutputStream(it).writeInt(length) }.toByteArray()
            val input = object : ByteArrayInputStream(prefix) {
                override fun read(b: ByteArray, off: Int, len: Int): Int = error("body must not be read")
            }
            assertThrows(IllegalArgumentException::class.java) { BleFrames.read(input) }
        }
        assertThrows(IllegalArgumentException::class.java) { BleFrames.write(ByteArrayOutputStream(), byteArrayOf()) }
    }
    @Test fun truncatedPrefixAndBodyFailRatherThanBeingCleanEof() {
        for (bytes in listOf(byteArrayOf(0), byteArrayOf(0, 0, 0, 3, 1))) {
            assertThrows(EOFException::class.java) { BleFrames.read(ByteArrayInputStream(bytes)) }
        }
    }
}
