//#pragma once
//#include "mpi/oneccl/base_allreduce.h"

//class CollectiveTree : public OneCCLAllreduce {
class CollectiveTree {
    public:

    CollectiveTree(EmberOneCCLAllreduceGenerator& allreduce, Params& params, int iterations);
    void configure();
    void generate(std::queue<EmberEvent*>& evQ);
    ~CollectiveTree();






    private:

    int m_world;
    int m_count;
    int m_rank;
    int loopIndex,iterations;


    EmberOneCCLAllreduceGenerator& allreduce;



    std::vector<void*>  m_bufV;

    std::vector<MessageRequest>  m_recvReqV;
    std::vector<MessageRequest>  m_sendReqV;

    size_t              m_bufLen;
    YYY* m_yyy;



    struct WaitUpState {
    WaitUpState() : count(0), state(Posting) {}
    unsigned int count;
    enum { Posting, Waiting, DoOp } state;
    void init() { state = Posting; count = 0; }
    };

    struct SendDownState {
    SendDownState() : count(0), state(Sending) {}
    unsigned int count;
    enum { Sending, Waiting } state;
    void init() { state = Sending; count = 0; }
    };

    WaitUpState m_waitUpState;
    SendDownState m_sendDownState;

    enum StateEnum {
    FOREACH_ENUM(GENERATE_ENUM)
    } m_state;



};


/*
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

*/