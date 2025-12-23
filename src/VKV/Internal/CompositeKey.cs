using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VKV.Internal;

public static class CompositeKey
{
    public static bool TryGenerate(
        ReadOnlyMemory<byte> sourceKey,
        int valueId,
        Span<byte> destination)
    {
        ref var ptr = ref MemoryMarshal.GetReference(destination);
        ptr = ref Unsafe.Add(ref ptr, sourceKey.Length);
        Unsafe.WriteUnaligned(ref ptr, valueId);
        return true;
    }
}
