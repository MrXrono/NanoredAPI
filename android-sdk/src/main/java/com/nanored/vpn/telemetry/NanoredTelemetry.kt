package com.nanored.vpn.telemetry

import android.content.Context
import android.content.SharedPreferences
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.net.wifi.WifiManager
import android.os.BatteryManager
import android.os.Build
import android.provider.Settings
import android.telephony.TelephonyManager
import android.util.DisplayMetrics
import android.util.Log
import kotlinx.coroutines.*
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import javax.net.ssl.HttpsURLConnection

/**
 * NanoredVPN Telemetry SDK
 *
 * Интеграция:
 * 1. NanoredTelemetry.init(context, "https://api.nanored.top")
 * 2. При подключении VPN: NanoredTelemetry.startSession(server, protocol)
 * 3. Во время работы: addSNI(), addDNS(), addAppTraffic(), addConnection()
 * 4. При отключении: endSession(bytesDown, bytesUp)
 * 5. Периодически вызывать flush() или включить autoFlush
 */
object NanoredTelemetry {

    private const val TAG = "NanoredTelemetry"
    private const val PREFS_NAME = "nanored_telemetry"
    private const val KEY_DEVICE_ID = "device_id"
    private const val KEY_API_KEY = "api_key"

    private lateinit var context: Context
    private lateinit var baseUrl: String
    private var deviceId: String? = null
    private var apiKey: String? = null
    private var currentSessionId: String? = null

    private val sniBuffer = ConcurrentLinkedQueue<SNIEntry>()
    private val dnsBuffer = ConcurrentLinkedQueue<DNSEntry>()
    private val appTrafficBuffer = ConcurrentLinkedQueue<AppTrafficEntry>()
    private val connectionBuffer = ConcurrentLinkedQueue<ConnectionEntry>()

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var heartbeatJob: Job? = null
    private var autoFlushJob: Job? = null

    // ==================== DATA CLASSES ====================

    data class SNIEntry(val domain: String, var hitCount: Int = 1, var bytesTotal: Long = 0)
    data class DNSEntry(val domain: String, val resolvedIp: String? = null, val queryType: String = "A", var hitCount: Int = 1)
    data class AppTrafficEntry(val packageName: String, val appName: String? = null, val bytesDown: Long = 0, val bytesUp: Long = 0)
    data class ConnectionEntry(val destIp: String, val destPort: Int, val protocol: String = "TCP", val domain: String? = null)

    // ==================== INIT ====================

    fun init(ctx: Context, apiBaseUrl: String, autoFlushIntervalSec: Long = 60) {
        context = ctx.applicationContext
        baseUrl = apiBaseUrl.trimEnd('/')

        val prefs = getPrefs()
        deviceId = prefs.getString(KEY_DEVICE_ID, null)
        apiKey = prefs.getString(KEY_API_KEY, null)

        scope.launch {
            if (apiKey == null) {
                register()
            }
        }

        // Auto flush
        if (autoFlushIntervalSec > 0) {
            autoFlushJob?.cancel()
            autoFlushJob = scope.launch {
                while (isActive) {
                    delay(autoFlushIntervalSec * 1000)
                    flush()
                }
            }
        }

        Log.d(TAG, "Initialized. Base URL: $baseUrl")
    }

    // ==================== REGISTER ====================

    private suspend fun register() {
        try {
            val androidId = Settings.Secure.getString(context.contentResolver, Settings.Secure.ANDROID_ID)
            val dm = context.resources.displayMetrics

            val body = JSONObject().apply {
                put("android_id", androidId)
                put("device_model", Build.MODEL)
                put("manufacturer", Build.MANUFACTURER)
                put("android_version", Build.VERSION.RELEASE)
                put("api_level", Build.VERSION.SDK_INT)
                put("app_version", getAppVersion())
                put("screen_resolution", "${dm.widthPixels}x${dm.heightPixels}")
                put("dpi", dm.densityDpi)
                put("language", Locale.getDefault().language)
                put("timezone", TimeZone.getDefault().id)
                put("is_rooted", isRooted())
                put("carrier", getCarrier())
                put("ram_total_mb", getRamMB())
            }

            val resp = post("/api/v1/client/register", body)
            if (resp != null) {
                deviceId = resp.optString("device_id")
                apiKey = resp.optString("api_key")
                getPrefs().edit()
                    .putString(KEY_DEVICE_ID, deviceId)
                    .putString(KEY_API_KEY, apiKey)
                    .apply()
                Log.d(TAG, "Registered. Device ID: $deviceId")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Register failed", e)
        }
    }

    // ==================== SESSION ====================

    fun startSession(serverAddress: String? = null, protocol: String? = null) {
        scope.launch {
            try {
                val body = JSONObject().apply {
                    put("server_address", serverAddress)
                    put("protocol", protocol)
                    put("network_type", getNetworkType())
                    put("wifi_ssid", getWifiSSID())
                    put("carrier", getCarrier())
                    put("latency_ms", JSONObject.NULL)
                    put("battery_level", getBatteryLevel())
                }

                val resp = post("/api/v1/client/session/start", body, auth = true)
                if (resp != null) {
                    currentSessionId = resp.optString("session_id")
                    startHeartbeat()
                    Log.d(TAG, "Session started: $currentSessionId")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Session start failed", e)
            }
        }
    }

    fun endSession(bytesDownloaded: Long, bytesUploaded: Long, connectionCount: Int = 0, reconnectCount: Int = 0) {
        val sessionId = currentSessionId ?: return
        scope.launch {
            // Flush remaining data
            flush()

            val body = JSONObject().apply {
                put("session_id", sessionId)
                put("bytes_downloaded", bytesDownloaded)
                put("bytes_uploaded", bytesUploaded)
                put("connection_count", connectionCount)
                put("reconnect_count", reconnectCount)
            }
            post("/api/v1/client/session/end", body, auth = true)
            currentSessionId = null
            stopHeartbeat()
            Log.d(TAG, "Session ended: $sessionId")
        }
    }

    private fun startHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (isActive && currentSessionId != null) {
                delay(120_000) // every 2 minutes
                try {
                    post("/api/v1/client/session/heartbeat", JSONObject(), auth = true)
                } catch (_: Exception) {}
            }
        }
    }

    private fun stopHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = null
    }

    // ==================== TELEMETRY COLLECTION ====================

    fun addSNI(domain: String, hitCount: Int = 1, bytesTotal: Long = 0) {
        sniBuffer.add(SNIEntry(domain, hitCount, bytesTotal))
    }

    fun addDNS(domain: String, resolvedIp: String? = null, queryType: String = "A", hitCount: Int = 1) {
        dnsBuffer.add(DNSEntry(domain, resolvedIp, queryType, hitCount))
    }

    fun addAppTraffic(packageName: String, appName: String? = null, bytesDown: Long = 0, bytesUp: Long = 0) {
        appTrafficBuffer.add(AppTrafficEntry(packageName, appName, bytesDown, bytesUp))
    }

    fun addConnection(destIp: String, destPort: Int, protocol: String = "TCP", domain: String? = null) {
        connectionBuffer.add(ConnectionEntry(destIp, destPort, protocol, domain))
    }

    fun reportError(errorType: String, message: String? = null, stacktrace: String? = null) {
        scope.launch {
            val body = JSONObject().apply {
                put("session_id", currentSessionId)
                put("error_type", errorType)
                put("message", message)
                put("stacktrace", stacktrace)
                put("app_version", getAppVersion())
            }
            post("/api/v1/client/error", body, auth = true)
        }
    }

    // ==================== FLUSH ====================

    fun flush() {
        scope.launch { flushInternal() }
    }

    private suspend fun flushInternal() {
        val sessionId = currentSessionId ?: return
        if (apiKey == null) return

        // SNI
        val sniEntries = drainBuffer(sniBuffer)
        if (sniEntries.isNotEmpty()) {
            val arr = JSONArray()
            sniEntries.forEach { e ->
                arr.put(JSONObject().apply {
                    put("domain", e.domain)
                    put("hit_count", e.hitCount)
                    put("bytes_total", e.bytesTotal)
                })
            }
            post("/api/v1/client/sni/batch", JSONObject().apply {
                put("session_id", sessionId)
                put("entries", arr)
            }, auth = true)
        }

        // DNS
        val dnsEntries = drainBuffer(dnsBuffer)
        if (dnsEntries.isNotEmpty()) {
            val arr = JSONArray()
            dnsEntries.forEach { e ->
                arr.put(JSONObject().apply {
                    put("domain", e.domain)
                    put("resolved_ip", e.resolvedIp)
                    put("query_type", e.queryType)
                    put("hit_count", e.hitCount)
                })
            }
            post("/api/v1/client/dns/batch", JSONObject().apply {
                put("session_id", sessionId)
                put("entries", arr)
            }, auth = true)
        }

        // App Traffic
        val appEntries = drainBuffer(appTrafficBuffer)
        if (appEntries.isNotEmpty()) {
            val arr = JSONArray()
            appEntries.forEach { e ->
                arr.put(JSONObject().apply {
                    put("package_name", e.packageName)
                    put("app_name", e.appName)
                    put("bytes_downloaded", e.bytesDown)
                    put("bytes_uploaded", e.bytesUp)
                })
            }
            post("/api/v1/client/app-traffic/batch", JSONObject().apply {
                put("session_id", sessionId)
                put("entries", arr)
            }, auth = true)
        }

        // Connections
        val connEntries = drainBuffer(connectionBuffer)
        if (connEntries.isNotEmpty()) {
            val arr = JSONArray()
            connEntries.forEach { e ->
                arr.put(JSONObject().apply {
                    put("dest_ip", e.destIp)
                    put("dest_port", e.destPort)
                    put("protocol", e.protocol)
                    put("domain", e.domain)
                })
            }
            post("/api/v1/client/connections/batch", JSONObject().apply {
                put("session_id", sessionId)
                put("entries", arr)
            }, auth = true)
        }
    }

    private fun <T> drainBuffer(queue: ConcurrentLinkedQueue<T>): List<T> {
        val list = mutableListOf<T>()
        while (true) {
            val item = queue.poll() ?: break
            list.add(item)
        }
        return list
    }

    // ==================== HTTP ====================

    private fun post(path: String, body: JSONObject, auth: Boolean = false): JSONObject? {
        return try {
            val url = URL("$baseUrl$path")
            val conn = url.openConnection() as HttpsURLConnection
            conn.requestMethod = "POST"
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Accept", "application/json")
            if (auth && apiKey != null) {
                conn.setRequestProperty("X-API-Key", apiKey)
            }
            conn.doOutput = true
            conn.connectTimeout = 10000
            conn.readTimeout = 10000

            val writer = OutputStreamWriter(conn.outputStream)
            writer.write(body.toString())
            writer.flush()
            writer.close()

            val code = conn.responseCode
            if (code in 200..299) {
                val reader = BufferedReader(InputStreamReader(conn.inputStream))
                val response = reader.readText()
                reader.close()
                JSONObject(response)
            } else {
                Log.w(TAG, "HTTP $code for $path")
                null
            }
        } catch (e: Exception) {
            Log.e(TAG, "Request failed: $path", e)
            null
        }
    }

    // ==================== DEVICE INFO ====================

    private fun getPrefs(): SharedPreferences =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    private fun getAppVersion(): String = try {
        context.packageManager.getPackageInfo(context.packageName, 0).versionName ?: "unknown"
    } catch (_: Exception) { "unknown" }

    private fun isRooted(): Boolean = try {
        Runtime.getRuntime().exec("su").destroy()
        true
    } catch (_: Exception) { false }

    private fun getCarrier(): String? = try {
        (context.getSystemService(Context.TELEPHONY_SERVICE) as? TelephonyManager)?.networkOperatorName
    } catch (_: Exception) { null }

    private fun getRamMB(): Int = try {
        val activityManager = context.getSystemService(Context.ACTIVITY_SERVICE) as android.app.ActivityManager
        val memInfo = android.app.ActivityManager.MemoryInfo()
        activityManager.getMemoryInfo(memInfo)
        (memInfo.totalMem / (1024 * 1024)).toInt()
    } catch (_: Exception) { 0 }

    private fun getNetworkType(): String {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val network = cm.activeNetwork ?: return "unknown"
        val caps = cm.getNetworkCapabilities(network) ?: return "unknown"
        return when {
            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "wifi"
            caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "mobile"
            caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "ethernet"
            else -> "other"
        }
    }

    private fun getWifiSSID(): String? = try {
        val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        @Suppress("DEPRECATION")
        val info = wm.connectionInfo
        val ssid = info.ssid?.replace("\"", "")
        if (ssid == "<unknown ssid>") null else ssid
    } catch (_: Exception) { null }

    private fun getBatteryLevel(): Int {
        val bm = context.getSystemService(Context.BATTERY_SERVICE) as BatteryManager
        return bm.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY)
    }
}
