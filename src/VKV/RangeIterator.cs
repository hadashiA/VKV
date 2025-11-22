// using System;
// using System.Collections;
// using System.Collections.Generic;
// using System.Threading;
// using System.Threading.Tasks;
// using VKV.BTree;
// using VKV.Internal;
//
// namespace VKV;
//
// public struct RangeIterator :
//     IAsyncEnumerable<ReadOnlyMemory<byte>>,
//     IAsyncEnumerator<ReadOnlyMemory<byte>>,
//     IEnumerable<ReadOnlyMemory<byte>>,
//     IEnumerator<ReadOnlyMemory<byte>>
// {
//     public static RangeIterator Empty => new();
//
//     public ReadOnlyMemory<byte> Current =>
//         currentPage!.Memory.Slice(currentValueOffset, currentValueLength);
//
//     ReadOnlyMemory<byte> IEnumerator<ReadOnlyMemory<byte>>.Current => Current;
//     ReadOnlyMemory<byte> IAsyncEnumerator<ReadOnlyMemory<byte>>.Current => Current;
//     object IEnumerator.Current => Current;
//
//     // query params
//     ReadOnlyMemory<byte> startKey;
//     ReadOnlyMemory<byte> endKey;
//     bool startKeyExclusive;
//     bool endKeyExclusive;
//
//     // iterator state
//     TreeWalker treeWalker;
//     IPageEntry? currentPage;
//     int currentNodeEntryCount = -1;
//     int currentIndex;
//     int currentValueOffset;
//     ushort currentValueLength;
//     CancellationToken cancellationToken;
//
//     internal RangeIterator(
//         TreeWalker treeWalker,
//         ReadOnlyMemory<byte> startKey,
//         ReadOnlyMemory<byte> endKey,
//         bool startKeyExclusive,
//         bool endKeyExclusive)
//     {
//         this.treeWalker = treeWalker;
//         this.startKey = startKey;
//         this.startKeyExclusive = startKeyExclusive;
//         this.endKey = endKey;
//         this.endKeyExclusive = endKeyExclusive;
//     }
//
//     public RangeIterator GetAsyncEnumerator(CancellationToken cancellationToken)
//     {
//         this.cancellationToken = cancellationToken;
//         return this;
//     }
//
//     public RangeIterator GetEnumerator() => this;
//
//     IAsyncEnumerator<ReadOnlyMemory<byte>> IAsyncEnumerable<ReadOnlyMemory<byte>>.GetAsyncEnumerator(
//         CancellationToken cancellationToken) =>
//         GetAsyncEnumerator(cancellationToken);
//
//     IEnumerator<ReadOnlyMemory<byte>> IEnumerable<ReadOnlyMemory<byte>>.GetEnumerator() => GetEnumerator();
//     IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
//     public void Dispose()
//     {
//         currentNodeEntryCount = -2;
//         currentPage.Release();
//     }
//
//     public void Reset()
//     {
//         currentNodeEntryCount = -2;
//     }
//
//     public ValueTask DisposeAsync()
//     {
//         Dispose();
//         return default;
//     }
//
//     public bool MoveNext()
//     {
//         // first item
//         if (currentPage is null)
//         {
//             var pageNumber = treeWalker.RootPageNumber;
//             var index = 0;
//
//             // find root
//             if (startKey.IsEmpty)
//             {
//                 PageNumber? nextPage = pageNumber;
//                 while (!TrySearch(
//                            nextPage.Value,
//                            startKey.Value.Span,
//                            startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
//                            out index,
//                            out nextPage))
//                 {
//                     if (!nextPage.HasValue) return RangeResult.Empty;
//
//                     await PageCache.LoadAsync(nextPage.Value, cancellationToken).ConfigureAwait(false);
//                     pageNumber = nextPage.Value;
//                 }
//             }
//             var header = currentPage.GetNodeHeader();
//             currentNodeEntryCount = header.EntryCount;
//             if (header.Kind != NodeKind.Leaf)
//             {
//                 throw new InvalidOperationException("Invalid node kind");
//             }
//             return currentIndex < currentNodeEntryCount;
//         }
//
//         // tail of node
//         if (currentIndex >= currentNodeEntryCount - 1)
//         {
//             var header = currentPage.GetNodeHeader();
//             // check right node exists
//             if (header.RightSiblingPageNumber.IsEmpty)
//             {
//                 return false;
//             }
//
//             currentPage.Release();
//             while (!pageCache.TryGet(header.RightSiblingPageNumber, out currentPage))
//             {
//                 pageCache.Load(header.RightSiblingPageNumber);
//             }
//
//             header = currentPage.GetNodeHeader();
//             currentNodeEntryCount = header.EntryCount;
//             if (header.Kind != NodeKind.Leaf)
//             {
//                 throw new InvalidOperationException("Invalid node kind");
//             }
//             currentIndex = 0;
//             if (currentNodeEntryCount < 0)
//             {
//                 return false;
//             }
//         }
//
//         currentIndex++;
//
//         var leafNode = new LeafNodeReader(currentPage.Memory.Span, currentNodeEntryCount);
//         leafNode.GetAt(currentIndex, out var key, out currentValueOffset, out currentValueLength);
//
//         // check end key
//         if (endKey.HasValue)
//         {
//             var keyComparer = keyEncoding.ToKeyComparer();
//             var compared = keyComparer.Compare(key, endKey.Value.Span);
//             if (compared > 0 || (!endKeyExclusive && compared == 0))
//             {
//                 return false;
//             }
//         }
//         return true;
//     }
//
//     public ValueTask<bool> MoveNextAsync()
//     {
//         throw new NotImplementedException();
//     }
// }
