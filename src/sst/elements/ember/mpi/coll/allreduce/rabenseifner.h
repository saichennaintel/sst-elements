//#pragma once
//#include "mpi/oneccl/base_allreduce.h"


class Rabenseifner {

	public:

	    Rabenseifner(EmberOneCCLAllreduceGenerator& allreduce, Params& params, int iterations);
	    void configure();
	    void generate(std::queue<EmberEvent*>& evQ);
	    ~Rabenseifner();


	private:

	    int m_world;
	    int m_compute;
	    int m_count;
	    int m_rank;
	    int m_newrank;
	    int m_pof2;
	    int m_rem;
	    int m_stages;
	    int loopIndex,iterations;
	    EmberOneCCLAllreduceGenerator& allreduce;

		bool m_verbose;

		int m_stage = 1;

	    void* m_sendBuf;
	    void* m_recvBuf;
	    std::vector<int> m_cnts, m_disps;
	    std::vector<MessageRequest> m_requests;

	    int getpof2();	
    };