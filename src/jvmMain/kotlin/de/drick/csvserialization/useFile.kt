package de.drick.csvserialization

import java.io.File

actual fun useFile(filePath: String, lineSequence: (Sequence<String>) -> Unit) {
    val file = File(filePath)
    file.bufferedReader().use {
        lineSequence(it.lineSequence())
    }
}
