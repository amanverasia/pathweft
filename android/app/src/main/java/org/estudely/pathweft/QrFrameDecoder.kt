package org.estudely.pathweft

import com.google.zxing.BinaryBitmap
import com.google.zxing.DecodeHintType
import com.google.zxing.NotFoundException
import com.google.zxing.ChecksumException
import com.google.zxing.FormatException
import com.google.zxing.PlanarYUVLuminanceSource
import com.google.zxing.common.HybridBinarizer
import com.google.zxing.qrcode.QRCodeReader

/**
 * Finds a Pathweft contact card in one camera frame's luminance (Y) plane.
 * Returns null when no QR code is found or when the QR is not a Pathweft card;
 * the card is then checked strictly by the Rust parser like a pasted card.
 */
class QrFrameDecoder {
    private val reader = QRCodeReader()
    private val hints = mapOf(DecodeHintType.TRY_HARDER to true)

    fun decodeCard(luminance: ByteArray, width: Int, height: Int, rowStride: Int = width): String? {
        val source = PlanarYUVLuminanceSource(luminance, rowStride, height, 0, 0, width, height, false)
        val text = try {
            reader.decode(BinaryBitmap(HybridBinarizer(source)), hints).text
        } catch (_: NotFoundException) {
            return null
        } catch (_: ChecksumException) {
            return null
        } catch (_: FormatException) {
            return null
        } finally {
            reader.reset()
        }
        return text.takeIf { it.startsWith(CARD_PREFIX) && it.length <= MAX_CARD_CHARS }
    }

    companion object {
        const val CARD_PREFIX = "pathweft:contact/"
        const val MAX_CARD_CHARS = 128
    }
}
