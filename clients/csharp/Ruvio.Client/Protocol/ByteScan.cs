using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ruvio.Client;

/// <summary>Finds a byte with a hardware vector compare, then a scalar tail.</summary>
internal static class ByteScan
{
    internal static int IndexOf(ReadOnlySpan<byte> data, byte needle)
    {
        if (!Vector.IsHardwareAccelerated || data.Length < Vector<byte>.Count)
        {
            return data.IndexOf(needle);
        }

        var target = new Vector<byte>(needle);
        int width = Vector<byte>.Count;
        ref byte origin = ref MemoryMarshal.GetReference(data);
        int offset = 0;

        while (offset + width <= data.Length)
        {
            var block = Unsafe.ReadUnaligned<Vector<byte>>(ref Unsafe.Add(ref origin, offset));
            var matches = Vector.Equals(block, target);
        
            if (matches != Vector<byte>.Zero)
            {
                for (int lane = 0; lane < width; lane++)
                {
                    if (matches[lane] != 0)
                    {
                        return offset + lane;
                    }
                }
            }

            offset += width;
        }

        int tail = data[offset..].IndexOf(needle);

        return tail < 0 ? -1 : offset + tail;
    }
}
