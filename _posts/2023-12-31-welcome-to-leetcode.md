---
layout: post
title:  "Welcome to Leetcode!"
date:   2023-12-31 21:22:54 +0530
categories: None
---

# 700. Search in a Binary Search Tree

`Intuition`:

The function aims to search for a node with a specific value (val) in a Binary Search Tree (BST). In a BST, for any given node, all nodes in its left subtree have values less than the node's value, and all nodes in its right subtree have values greater than the node's value. The intuition behind the search is to recursively navigate the tree based on the comparison of the target value with the current node's value.

# Approach:
`Base Case`:

If the current node is None, return None (indicating that the value was not found).
`Search Comparison`:

Compare the target value (val) with the value of the current node (node.val).
If they are equal, return the current node, as it is the node with the target value.
If the target value is less than the current node's value, recursively search in the left subtree.
If the target value is greater than the current node's value, recursively search in the right subtree.

`Recursive Search`:

The recursive search continues until the target value is found or a leaf node is reached.

`Return Result`:

Return the node where the target value is found or None if not found.

# Complexity:

`Time Complexity`:

The time complexity of the search in a BST is O(h), where h is the height of the tree.
In the worst case, the height of the tree is equal to the number of nodes in the tree, resulting in O(n) time complexity (for a skewed tree).

`Space Complexity`:

The space complexity is O(h) due to the recursive calls on the call stack.
In the worst case, when the tree is skewed, the space complexity is O(n).

`code snippets`:

{% highlight ruby %}

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
       self.val = val
       self.left = left
       self.right = right
class Solution:
    def searchBST(self, root: Optional[TreeNode], val: int) -> Optional[TreeNode]:
        def search(node):
            if not node:
                return None
            if node.val == val:
                return node
            
            if node.val > val:
                if node.left:
                    return search(node.left)
                else:
                    return None
            else:
                if node.right:
                    return search(node.right)
                else:
                    return None
                
        return search(root)
{% endhighlight %}