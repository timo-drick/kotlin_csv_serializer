package de.drick.csvserialization

import java.io.File

actual fun useFile(filePath: String, block: (Sequence<String>) -> Unit) {
    val file = File(filePath)
    file.bufferedReader().use {
        block(it.lineSequence())
    }
}
