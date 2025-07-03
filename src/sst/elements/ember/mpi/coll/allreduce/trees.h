#pragma once

#include <iostream>
#include <stddef.h>
#include <sys/types.h>
#include <vector>


#undef FOREACH_ENUM
#define FOREACH_ENUM(NAME) \
    NAME( WaitUp ) \
    NAME( SendUp ) \
    NAME( WaitDown ) \
    NAME( SendDown ) \
    NAME( Exit ) \

#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,

class YYY {

  public:
    YYY( int degree, int myRank, int size, int root  ) :
        m_degree( degree ),
        m_myRank( myRank ),
        m_size( size ),
        m_numChildren(0),
        m_myVirtRank( myRank ),
        m_root( root ),
        m_parent( -1 )
    {
        if ( root > 0 ) {
            if ( root == m_myRank ) {
                m_myVirtRank = 0;
            } else if ( 0 == m_myRank )  {
                m_myVirtRank = root;
            }
        }

        for ( int i = 0; i < m_degree; i++ ) {
            if ( calcChild( i ) < m_size ) {
                ++m_numChildren;
            }
        }

        if ( m_myVirtRank > 0 ) {
            int tmp = m_myVirtRank % m_degree;
            tmp = 0 == tmp ? m_degree : tmp ;
            m_parent = (m_myVirtRank - tmp ) / m_degree;

            if ( m_parent == 0 ) {
                m_parent = m_root;
            } else if ( m_parent == m_root ) {
                m_parent = 0;
            }
        }
    }

    int myRank() { return m_myRank; }

    int size() { return m_size; }

    int parent() { return m_parent; }

    size_t numChildren() { return m_numChildren; }

    int calcChild( int i ) {
        int child = (m_myVirtRank * m_degree) + i + 1;
        // ummm, child can never be 0
        if ( child == 0 ) {
            child = m_root;
        }  else if ( child == m_root ) {
            child = 0;
        }
        return child;
    }

    private:

    int m_degree;
    int m_myRank;
    int m_size;
    int m_numChildren;
    int m_myVirtRank;
    int m_root;
    int m_parent;
    
};

// Double Binary Tree

class ccl_bin_tree {
public:
    ccl_bin_tree(int comm_size, int rank, bool is_main = true)
            : comm_size(comm_size),
              rank(rank),
              is_main(is_main) {
        calc_height(is_main);

        if (rank == default_root) {
            p = -1;
            l = -1;
            if (is_main) {
                r = height > 0 ? 1 << (height - 1) : -1;
            }
            else {
                if (comm_size == 1 << height) {
                    r = height > 0 ? (1 << height) - 1 : -1;
                }
                else {
                    r = height > 0 ? (1 << (height - 1)) - 1 : -1;
                }
            }
            return;
        }

        calc_parent();
        if (height > 0) {
            calc_left();
            calc_right();
        }
    }

    ccl_bin_tree(const ccl_bin_tree& other) = default;
    ccl_bin_tree& operator=(const ccl_bin_tree& other) = default;

    int left() const {
        return l;
    }

    int right() const {
        return r;
    }

    int parent() const {
        return p;
    }

    int getchildren() {

        int tmp = 0;

        tmp = (left() == -1) ? 0 : 1 ;
        tmp += (right() == -1) ? 0 : 1 ;

        return tmp ;
    }

    int get_child(int id) {

        int children = getchildren();

        if (children == 0 ) {

            return -1;
        }

        else if (children == 1) {

            if (left() == -1) {
                return right();
            }

            else if (right() == -1) {
                return left();
            }
        }

        else if (children == 2) {

            if (id == 0) {
                return left();
            }

            if (id == 1) {
                return right();
            }

            else {
                return -1;
            }
        }


        else {

            return -1;
        }

        return -1;


    }


    ccl_bin_tree copy_with_new_root(int new_root) const {
        ccl_bin_tree copy(*this);
        int root = static_cast<int>(new_root);

        //if current node will become a new root or node was a default root - the tree must be reconstruced
        if (copy.rank == root || copy.rank == default_root) {
            //create part of tree with the default root
            copy = ccl_bin_tree(static_cast<int>(comm_size),
                                copy.rank == default_root ? root : default_root,
                                is_main);
            copy.rank = root;
        }

        //swap default root with new root in any of left/right/parent nodes
        copy.reset_connections(root);

        return copy;
    }

    friend std::ostream& operator<<(std::ostream& str, const ccl_bin_tree& tree) {
        str << "parent " << tree.p << " -> rank " << tree.rank << " -> [left " << tree.l
            << ", right " << tree.r << "]";
        return str;
    }

private:
    void reset_connections(int new_root) {
        swap_if_any_of(p, default_root, new_root);
        swap_if_any_of(l, default_root, new_root);
        swap_if_any_of(r, default_root, new_root);
    }

    static void swap_if_any_of(int& node, int val1, int val2) {
        if (node == val1) {
            node = val2;
        }
        else if (node == val2) {
            node = val1;
        }
    }

    void calc_height(bool main_tree) {
        if (main_tree || rank == default_root) {
            while ((rank & (1 << height)) == 0 && (1 << height) < comm_size) {
                ++height;
            }
        }
        else {
            while ((rank & (1 << height)) != 0 && (1 << height) < comm_size) {
                ++height;
            }
        }
    }

    void calc_parent() {
        //find a parent using height, assume that rank is a right child
        int possible_parent_as_left = rank + (1 << height);
        //right child has a bit `1` a the position `height + 1` due to it is calculated as `parent + 2^(heightP-1)`
        //where heightP is parent's height i.e height + 1

        if ((rank & (1 << (height + 1))) ||
            //parent of the left rank is always bigger than its parent, check that we do not exceed comm size
            possible_parent_as_left > comm_size - 1) {
            //this is right child
            p = rank - (1 << height);
            if (p < 0) {
                p = 0;
            }
        }
        else {
            p = possible_parent_as_left;
        }
    }

    void calc_left() {
        l = rank - (1 << (height - 1));
        if (l <= 0) {
            l = -1;
        }
    }

    void calc_right() {
        r = rank + (1 << (height - 1));
        int limit = comm_size - 1;

        if (r > limit) {
            auto height_tmp = height;
            //need to decrease height to find most suitable right -- topmost right leaf case
            do {
                --height_tmp;
                if (height_tmp == 0) {
                    r = -1;
                    break;
                }
                r = rank + (1 << (height_tmp - 1));

            } while (r > limit);
        }
    }

    int comm_size;
    int rank;
    int height = 0;
    int p = -1;
    int l = -1;
    int r = -1;
    bool is_main;

    static const int default_root = 0;
};

class ccl_double_tree {
public:
    ccl_double_tree(int comm_size, int rank)
            : t1(comm_size, rank, true),
              t2(comm_size, rank, false) {
        //LOG_DEBUG("T1: ", t1);
        //LOG_DEBUG("T2: ", t2);
    }

    /**
     * Binary tree which consists of the current rank as a node and possible parent, left and right children.
     * Even ranks numbers are always inner nodes, odd rank numbers are always leaves
     * @return binary tree t1
     */
    ccl_bin_tree& T1() {
        return t1;
    }

    /**
     * Binary tree which consists of the current rank as a node and possible parent, left and right children.
     * Even ranks numbers are always leaves, odd rank numbers are always inner nodes
     * @return binary tree t2
     */
    ccl_bin_tree& T2() {
        return t2;
    }

    ccl_double_tree copy_with_new_root(int new_root) const {
        return ccl_double_tree(t1.copy_with_new_root(new_root), t2.copy_with_new_root(new_root));
    }



private:
    ccl_double_tree(ccl_bin_tree t1, ccl_bin_tree t2) : t1(t1), t2(t2) {}

    ccl_bin_tree t1;
    ccl_bin_tree t2;

    


};


//NCCL Double Binary Tree

class nccl_bin_tree {

    public:

        nccl_bin_tree(uint32_t comm_size, uint32_t rank ) : comm_size(comm_size), rank(rank) {

            int bit,parent,left,right;

            for(bit = 1 ; bit < comm_size ; bit<<=1) {

                if (bit & rank) break;
            }

            if (rank == 0) {
                p = -1;
                l = -1;
                r = comm_size > 1 ? bit >> 1 : -1 ;

                return;

            }

            parent = (rank ^ bit) | (bit << 1);

            if (parent >= comm_size ) parent = (rank ^ bit);
            p = parent;

            int lowbit = bit >> 1 ;
            left = lowbit == 0 ? -1 : rank-lowbit ;
            right = lowbit == 0 ? -1 : rank+lowbit ;

            while (right >= comm_size) {

                right = lowbit == 0 ? -1 : rank+lowbit ;
                lowbit >>=  1;
            }

            l = left ;
            r = right ;


        }

        nccl_bin_tree(const nccl_bin_tree& other) = default;
        nccl_bin_tree& operator=(const nccl_bin_tree& other) = default;


        void setParent(int val) {
            p = val;
        }

        void setLeftChild(int val) {
            l = val;
        }

        void setRightChild(int val) {
            r = val;
        }

        //int getParent() const {
        int parent() const {
            return p;
        }

        //int getLeftChild() const { 
        int left() const {
            return l;
        }

        //int getRightChild() const {
        int right() const {
            return r;
        }

        int getchildren() {

            int tmp = 0;

            tmp = (left() == -1) ? 0 : 1 ;
            tmp += (right() == -1) ? 0 : 1 ;

            return tmp ;

        }

        int get_child(int id) {

            int children = getchildren();

            if (children == 0 ) {

                return -1;
            }

            else if (children == 1) {

                if (left() == -1) {
                    return right();
                }

                else if (right() == -1) {
                    return left();
                }
            }

            else if (children == 2) {

                if (id == 0) {
                    return left();
                }

                if (id == 1) {
                    return right();
                }

                else {
                    return -1;
                }
            }


            else {

                return -1;
            }

            return -1;


        }

    private:

        uint32_t comm_size;
        uint32_t rank;
        int p = -1;
        int l = -1;
        int r = -1;

        

};

class nccl_double_tree {

    public:

        nccl_double_tree(int comm_size, int rank) : t1(comm_size, rank), t2(comm_size, rank) {

            if (comm_size % 2 == 1){
                //Shift
                int shiftrank = (rank -1 + comm_size) % comm_size ;
                int parent,left,right ;
                t2 = nccl_bin_tree(comm_size,shiftrank);
                int p1,c1,c2;
                int tmp;
                /*
                p1 = t2.getParent();
                c1 = t2.getLeftChild();
                c2 = t2.getRightChild();
                */

                p1 = t2.parent();
                c1 = t2.left();
                c2 = t2.right();

                tmp = p1 == -1 ? -1 : (p1 + 1) % comm_size ;
                t2.setParent(tmp);
                tmp = c1 == -1 ? -1 : (c1 + 1) % comm_size ;
                t2.setLeftChild(tmp);
                tmp = c2 == -1 ? -1 : (c2 + 1) % comm_size ;
                t2.setRightChild(tmp);


            }
            else {
                //Mirror
                t2 = nccl_bin_tree(comm_size,comm_size - rank -1);

                int p1,c1,c2;
                int tmp;
                /*
                p1 = t2.getParent();
                c1 = t2.getLeftChild();
                c2 = t2.getRightChild();
                */

                p1 = t2.parent();
                c1 = t2.left();
                c2 = t2.right();

                tmp = p1 == -1 ? -1 : comm_size - 1 - p1 ;
                t2.setParent(tmp);
                tmp = c1 == -1 ? -1 : comm_size - 1 - c1 ;
                t2.setLeftChild(tmp);
                tmp = c2 == -1 ? -1 : comm_size - 1 - c2 ;
                t2.setRightChild(tmp);



            }


        }

        nccl_bin_tree& T1()  {
        return t1;
        }

        nccl_bin_tree& T2() {
        return t2;
        }


    private:

        nccl_double_tree(nccl_bin_tree t1, nccl_bin_tree t2) : t1(t1), t2(t2) {}

        nccl_bin_tree t1;
        nccl_bin_tree t2;


};

