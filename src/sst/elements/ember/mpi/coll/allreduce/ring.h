//#pragma once
//#include "mpi/oneccl/base_allreduce.h"

//class Ring : public OneCCLAllreduce {
class Ring {
    private:

    uint32_t m_world ;
    uint32_t m_count ;
    uint32_t m_rank ;
    int m_minchunksize ;
    int m_chunkcount ;
    double m_memBW ;
    bool m_dochunks ;
    bool inplace ;

    uint32_t m_mainblock_count ;
    uint32_t m_lastblock_count ;
    std::vector<size_t> m_recv_counts ;
    std::vector<MessageRequest> m_requests ;
    
    void* m_recvBuf ;
    void* m_sendBuf ;

    int loopIndex,iterations;

    EmberOneCCLAllreduceGenerator& allreduce;

    public:

    Ring(EmberOneCCLAllreduceGenerator& allreduce, Params& params, int iterations, bool inplace);
    void configure();
    void generate(std::queue<EmberEvent*>&);
    double getCopyTimens(size_t);
    double getReduceTimens(size_t);
    void ring_reduce_scatter(std::queue<EmberEvent*>& );
    void ring_allgatherv(std::queue<EmberEvent*>& );
    ~Ring();






};
