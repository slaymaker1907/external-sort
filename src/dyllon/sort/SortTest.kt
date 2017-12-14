package dyllon.sort

import java.io.DataInput
import java.io.DataOutput
import java.util.*

object ByteSerializer : Serializer<Byte> {
    override fun serialize(obj: Byte, out: DataOutput) {
        out.writeByte(obj.toInt())
    }

    override fun deserialize(inp: DataInput): Byte {
        return inp.readByte()
    }
}

fun main(args: Array<String>) {
    val maxSize = 10_000_000_000L
    val largeIt = object : Iterator<Byte> {
        private var size = 0L
        private val gen = Random()
        private val buffer = ByteArray(1)

        override fun hasNext(): Boolean {
            return size < maxSize
        }

        override fun next(): Byte {
            gen.nextBytes(buffer)
            size++
            return buffer[0]
        }
    }

    SmartIterator(largeIt, ByteSerializer).use {
        it.removeDuplicates(Comparator.naturalOrder())
        while (it.hasNext()) {
            println(it.next())
        }
    }
}