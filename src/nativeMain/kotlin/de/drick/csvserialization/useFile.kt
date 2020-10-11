package de.drick.csvserialization

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.fclose
import platform.posix.fgets
import platform.posix.fopen

actual fun useFile(filePath: String, lineSequence: (Sequence<String>) -> Unit) {
    val file = fopen(filePath, "r") ?: throw Exception("cannot open input file: $filePath")
    try {
        val lineSequence = sequence<String> {
            memScoped {
                val bufferLength = 64 * 1024
                val buffer = allocArray<ByteVar>(bufferLength)
                while (true) {
                    val nextLine = fgets(buffer, bufferLength, file)?.toKString()
                    if (nextLine.isNullOrEmpty()) break
                    yield(nextLine)
                }
            }
        }
        lineSequence(lineSequence)
    } finally {
        fclose(file)
    }
}
