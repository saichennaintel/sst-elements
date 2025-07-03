//#pragma once
//#include "mpi/oneccl/base_allreduce.h"

class OneCCLDoubleTree {

    private:

    uint32_t m_world ;
    uint32_t m_count ;
    uint32_t m_rank ;
    bool inplace ;
    
    std::vector<void*>  t1_bufV;
    std::vector<void*> t2_bufV;

    size_t t1_work_count;
    size_t t2_work_count;
    int t1_children = 0;
    int t2_children = 0;
    
    //void* t1_bufV, t2_bufV ;
    //void* t1_sendBuf, t2_sendBuf ;

    int loopIndex,iterations;

    EmberOneCCLAllreduceGenerator& allreduce;

    


    std::vector<MessageRequest>  t1_recvReqV;
    std::vector<MessageRequest> t2_recvReqV;
    std::vector<MessageRequest>  t1_sendReqV;
    std::vector<MessageRequest> t2_sendReqV;

    std::vector<MessageRequest> t1_sendrecvReqV;
    std::vector<MessageRequest> t2_sendrecvReqV;


    size_t t1_bufLen;
    size_t t2_bufLen;


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

    WaitUpState t1_waitUpState, t2_waitUpState;
    SendDownState t1_sendDownState, t2_sendDownState;

    enum StateEnum {
    FOREACH_ENUM(GENERATE_ENUM)
    } t1_state, t2_state;


    ccl_double_tree* d_tree ;

    //nccl_double_tree* d_tree ;


    
    void reduce_broadcast(std::queue<EmberEvent*>&, int);
    void reduce_broadcastv2(std::queue<EmberEvent*>&, int);
    void reduce_broadcastv3(std::queue<EmberEvent*>&, int);
    void reduce_broadcastv4(std::queue<EmberEvent*>&, int);
    void graphinfo(int);



    public:

    OneCCLDoubleTree(EmberOneCCLAllreduceGenerator& allreduce, Params& params, int iterations);
    void configure();
    void generate(std::queue<EmberEvent*>&);
    //double getCopyTimens(size_t);
    //double getReduceTimens(size_t);
    ~OneCCLDoubleTree();

    


};
