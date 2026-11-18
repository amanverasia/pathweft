package org.estudely.pathweft

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper

class PathweftService : Service() {
    companion object { @Volatile var active = false; private set }
    private val handler = Handler(Looper.getMainLooper())
    private val poll = object : Runnable {
        override fun run() { NodeController.poll(); handler.postDelayed(this, 500) }
    }
    override fun onCreate() {
        super.onCreate()
        NodeController.initialize(this)
        val manager = getSystemService(NotificationManager::class.java)
        manager.createNotificationChannel(NotificationChannel("network", "Pathweft network", NotificationManager.IMPORTANCE_LOW))
        val open = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE)
        val stop = PendingIntent.getService(this, 1, Intent(this, PathweftService::class.java).setAction("STOP"), PendingIntent.FLAG_IMMUTABLE)
        val notification = Notification.Builder(this, "network").setSmallIcon(R.drawable.ic_pathweft)
            .setContentTitle("Pathweft network is running").setContentText("Local connections and relay service")
            .setContentIntent(open).setOngoing(true).addAction(Notification.Action.Builder(null, "Stop", stop).build()).build()
        if (Build.VERSION.SDK_INT >= 29) startForeground(1, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE)
        else startForeground(1, notification)
        active = true
        handler.post(poll)
    }
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == "STOP") stopSelf()
        else NodeController.submit("start") { response -> if (!response.optBoolean("ok")) stopSelf() }
        return START_NOT_STICKY
    }
    override fun onDestroy() {
        handler.removeCallbacks(poll)
        NodeController.submit("stop")
        active = false
        stopForeground(STOP_FOREGROUND_REMOVE)
        super.onDestroy()
    }
    override fun onBind(intent: Intent?): IBinder? = null
}
