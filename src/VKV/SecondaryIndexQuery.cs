using VKV.BTree;

namespace VKV;

public readonly struct SecondaryIndexQuery
{
    readonly TreeWalker tree;

    internal SecondaryIndexQuery(TreeWalker tree)
    {
        this.tree = tree;
    }

    // WIP
}
