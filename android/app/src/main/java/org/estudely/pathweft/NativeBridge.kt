package org.estudely.pathweft

object NativeBridge {
    init { System.loadLibrary("pathweft_android") }
    @JvmStatic external fun call(directory: String, operation: String, payload: String): String
}
