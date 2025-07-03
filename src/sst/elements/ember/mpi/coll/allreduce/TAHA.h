//#pragma once
//#include "mpi/oneccl/base_allreduce.h"


//class TAHA : public OneCCLAllreduce {
class TAHA {

    private:

    int m_world;
    int m_count;
    int m_rank;
    int m_tiles_per_xpu;
    int m_xpus_per_supernode;
    int m_supernode_count;
    int loopIndex,iterations;
    std::vector<int> m_supernode_neighbors;
    std::vector<MessageRequest> m_requests_phase1;
    std::vector<MessageRequest> m_requests_phase2;
    std::vector<MessageRequest> m_requests_phase3;
    std::vector<MessageRequest> m_requests_phase4;
    std::vector<MessageRequest> requests_phase1;
    std::vector<MessageRequest> requests_phase2;

    std::string m_intersupernode_nicallreduce_algo;

    Communicator m_intersupernodeComm;

    EmberOneCCLAllreduceGenerator& allreduce;

    void getlocaltileID();
    int getlocaltileID(int);
    void getglobalXPUID();
    int getglobalXPUID(int);
    void getsupernodeID();
    int getsupernodeID(int);
    void getlocalXPUID();
    int getlocalXPUID(int);

    //std::vector<void*> m_recvBuf;
    void* m_recvBuf;
    void* m_nicresultBuf;

    int m_localtileID;
    int m_tileneighbor;
    int m_supernodeID;
    int m_localXPUID;

    std::vector<int> m_intraxpu_cnts,m_intraxpu_disps;
    std::vector<int> m_intrasupernode_cnts,m_intrasupernode_disps;
    int m_intersupernode_cnts, m_intersupernode_disps;
    std::vector<int> m_intersupernode_neighbors;


    public:

    TAHA(EmberOneCCLAllreduceGenerator& allreduce, Params& params, int iterations);
    void configure();
    void generate(std::queue<EmberEvent*>& evQ);
    void intersupernode_nicallreduce(std::queue<EmberEvent*>& evQ);
    void rabenseifner_nicallreduce(std::queue<EmberEvent*>& evQ);
    ~TAHA();



};
