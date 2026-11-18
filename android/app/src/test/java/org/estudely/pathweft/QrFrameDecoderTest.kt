package org.estudely.pathweft

import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel
import org.junit.Assert.*
import org.junit.Test

class QrFrameDecoderTest {
    private val card = "pathweft:contact/1/9bac8e2946084555d8867adddc9a9f6aa1d3f1ea6194eabd993b544f809dce29/4892/tester"

    /** A grey camera-like frame with the QR drawn at an offset, plus row padding like real camera planes. */
    private fun frame(text: String, width: Int = 640, height: Int = 480, rowStride: Int = 704): ByteArray {
        val matrix = QRCodeWriter().encode(text, BarcodeFormat.QR_CODE, 300, 300,
            mapOf(EncodeHintType.ERROR_CORRECTION to ErrorCorrectionLevel.M, EncodeHintType.MARGIN to 4))
        val bytes = ByteArray(rowStride * height) { 0x70 }
        for (y in 0 until height) for (x in 0 until width) {
            val qx = x - 170; val qy = y - 90
            val inside = qx in 0 until matrix.width && qy in 0 until matrix.height
            bytes[y * rowStride + x] = when {
                !inside -> 0x70
                matrix.get(qx, qy) -> 0x10
                else -> 0xE0.toByte()
            }
        }
        return bytes
    }

    @Test fun decodesAPathweftCardFromAPaddedFrame() {
        assertEquals(card, QrFrameDecoder().decodeCard(frame(card), 640, 480, 704))
    }

    @Test fun ignoresOtherQrCodesAndEmptyFrames() {
        val decoder = QrFrameDecoder()
        assertNull(decoder.decodeCard(frame("https://example.com/not-a-card"), 640, 480, 704))
        assertNull(decoder.decodeCard(ByteArray(704 * 480) { 0x70 }, 640, 480, 704))
        assertNull(decoder.decodeCard(frame("pathweft:contact/" + "x".repeat(200)), 640, 480, 704))
    }
}
