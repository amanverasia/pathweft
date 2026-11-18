package org.estudely.pathweft

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.ImageFormat
import android.graphics.SurfaceTexture
import android.hardware.camera2.CameraCaptureSession
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraDevice
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CaptureRequest
import android.media.ImageReader
import android.os.Bundle
import android.os.Handler
import android.os.HandlerThread
import android.util.Size
import android.view.Gravity
import android.view.Surface
import android.view.TextureView
import android.widget.FrameLayout
import android.widget.TextView
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Full-screen camera that returns the first Pathweft contact card it sees (Camera2, no extra libraries
 * beyond ZXing core). The caller requests the CAMERA permission before starting it.
 */
class ScannerActivity : Activity() {
    private lateinit var preview: TextureView
    private var thread: HandlerThread? = null
    private var handler: Handler? = null
    private var camera: CameraDevice? = null
    private var session: CameraCaptureSession? = null
    private var reader: ImageReader? = null
    private val decoder = QrFrameDecoder()
    private val done = AtomicBoolean(false)

    override fun onCreate(state: Bundle?) {
        super.onCreate(state)
        preview = TextureView(this).apply { id = R.id.scanner_preview }
        setContentView(FrameLayout(this).apply {
            addView(preview)
            addView(TextView(this@ScannerActivity).apply {
                id = R.id.scanner_hint
                text = "Point the camera at the other person's Pathweft QR code"
                setTextColor(0xFFFFFFFF.toInt()); setBackgroundColor(0x99000000.toInt())
                setPadding(32, 24, 32, 24); gravity = Gravity.CENTER
            }, FrameLayout.LayoutParams(FrameLayout.LayoutParams.MATCH_PARENT, FrameLayout.LayoutParams.WRAP_CONTENT, Gravity.BOTTOM))
        })
    }

    override fun onResume() {
        super.onResume()
        if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) { finishWith(null); return }
        thread = HandlerThread("pathweft-scanner").also { it.start(); handler = Handler(it.looper) }
        if (preview.isAvailable) open() else preview.surfaceTextureListener = object : TextureView.SurfaceTextureListener {
            override fun onSurfaceTextureAvailable(s: SurfaceTexture, w: Int, h: Int) = open()
            override fun onSurfaceTextureSizeChanged(s: SurfaceTexture, w: Int, h: Int) {}
            override fun onSurfaceTextureDestroyed(s: SurfaceTexture) = true
            override fun onSurfaceTextureUpdated(s: SurfaceTexture) {}
        }
    }

    override fun onPause() {
        session?.close(); session = null
        camera?.close(); camera = null
        reader?.close(); reader = null
        thread?.quitSafely(); thread = null; handler = null
        super.onPause()
    }

    @SuppressLint("MissingPermission") // checked in onResume
    private fun open() {
        val manager = getSystemService(CameraManager::class.java)
        val id = manager.cameraIdList.firstOrNull {
            manager.getCameraCharacteristics(it).get(CameraCharacteristics.LENS_FACING) == CameraCharacteristics.LENS_FACING_BACK
        } ?: manager.cameraIdList.firstOrNull() ?: run { finishWith(null); return }
        val sizes = manager.getCameraCharacteristics(id).get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP)
            ?.getOutputSizes(ImageFormat.YUV_420_888).orEmpty()
        val size = sizes.minByOrNull { kotlin.math.abs(it.width * it.height - 1280 * 720) } ?: Size(640, 480)
        val frames = ImageReader.newInstance(size.width, size.height, ImageFormat.YUV_420_888, 2)
        reader = frames
        frames.setOnImageAvailableListener({ r ->
            val image = r.acquireLatestImage() ?: return@setOnImageAvailableListener
            try {
                if (done.get()) return@setOnImageAvailableListener
                val plane = image.planes[0]
                val bytes = ByteArray(plane.buffer.remaining()).also { plane.buffer.get(it) }
                val card = decoder.decodeCard(bytes, image.width, image.height, plane.rowStride)
                if (card != null && done.compareAndSet(false, true)) runOnUiThread { finishWith(card) }
            } finally { image.close() }
        }, handler)
        manager.openCamera(id, object : CameraDevice.StateCallback() {
            override fun onOpened(device: CameraDevice) {
                camera = device
                val texture = preview.surfaceTexture ?: return
                texture.setDefaultBufferSize(size.width, size.height)
                val surfaces = listOf(Surface(texture), frames.surface)
                @Suppress("DEPRECATION")
                device.createCaptureSession(surfaces, object : CameraCaptureSession.StateCallback() {
                    override fun onConfigured(s: CameraCaptureSession) {
                        session = s
                        val request = device.createCaptureRequest(CameraDevice.TEMPLATE_PREVIEW).apply {
                            surfaces.forEach { addTarget(it) }
                            set(CaptureRequest.CONTROL_AF_MODE, CaptureRequest.CONTROL_AF_MODE_CONTINUOUS_PICTURE)
                        }.build()
                        s.setRepeatingRequest(request, null, handler)
                    }
                    override fun onConfigureFailed(s: CameraCaptureSession) = finishWith(null)
                }, handler)
            }
            override fun onDisconnected(device: CameraDevice) { device.close() }
            override fun onError(device: CameraDevice, error: Int) { device.close(); runOnUiThread { finishWith(null) } }
        }, handler)
    }

    private fun finishWith(card: String?) {
        if (isFinishing) return
        setResult(if (card != null) RESULT_OK else RESULT_CANCELED, card?.let { Intent().putExtra(EXTRA_CARD, it) })
        finish()
    }

    companion object { const val EXTRA_CARD = "card" }
}
