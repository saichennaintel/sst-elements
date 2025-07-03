// Copyright 2009-2022 NTESS. Under the terms
// of Contract DE-NA0003525 with NTESS, the U.S.
// Government retains certain rights in this software.
//
// Copyright (c) 2009-2022, NTESS
// All rights reserved.
//
// Portions are copyright of other developers:
// See the file CONTRIBUTORS.TXT in the top level directory
// of the distribution for more information.
//
// This file is part of the SST software package. For license
// information, see the LICENSE file in the top level directory of the
// distribution.

//Author: Sai Prabhakar Rao Chenna

//Topology aware Allreduce: Falcon shores based Topology-Aware Hierarchical Allreduce (TAHA) algorithm
//Original algoritm was developed by Maria Garzaran

#include <sst_config.h>
#include "../motifs/emberonecclallreduce.h"

//#include "collectivetree.h"

using namespace SST::Ember;
using namespace SST::Hermes::MP;

EmberOneCCLAllreduceGenerator::TAHA::TAHA(EmberOneCCLAllreduceGenerator& parent, Params& params, int iter) : allreduce(parent) , loopIndex(0) , iterations(iter) {

    //1. Load the default and topology specific parameters
    m_world = allreduce.size();
    m_count = (uint32_t) params.find("arg.count", 1);
    m_rank = allreduce.rank();
    m_tiles_per_xpu = (uint32_t) params.find("arg.tpx", 2);
    m_xpus_per_supernode = (uint32_t) params.find("arg.xps", 4);
    //m_supernode_count = (uint32_t) params.find("arg.snc",4);
    m_supernode_count = (uint32_t) params.find("arg.snc",16);

    m_intersupernode_nicallreduce_algo = params.find<std::string>("arg.nicallreduce_algo","rabenseifner");


    //2. Check the default assumptions
    assert(m_count >= m_world);
    //assert(m_world == m_tiles_per_xpu*m_xpus_per_supernode*m_supernode_count);


}

/*
int EmberOneCCLAllreduceGenerator::TAHA::getlocaltileID(){
     
    return (m_rank % m_tiles_per_xpu);  

}
*/

int EmberOneCCLAllreduceGenerator::TAHA::getlocaltileID(int rank){
     
    return (rank % m_tiles_per_xpu);  

}

/*
int EmberOneCCLAllreduceGenerator::TAHA::getglobalXPUID(){
    return int(m_rank/m_tiles_per_xpu);
}
*/

int EmberOneCCLAllreduceGenerator::TAHA::getglobalXPUID(int rank){
    return int(rank/m_tiles_per_xpu);
}

/*
int EmberOneCCLAllreduceGenerator::TAHA::getsupernodeID(){
    return int(m_rank/(m_tiles_per_xpu*m_xpus_per_supernode));
}
*/

int EmberOneCCLAllreduceGenerator::TAHA::getsupernodeID(int rank){
    return int(rank/(m_tiles_per_xpu*m_xpus_per_supernode));
}

/*
int EmberOneCCLAllreduceGenerator::TAHA::getlocalXPUID(){
    return getglobalXPUID()%m_xpus_per_supernode;
}
*/

int EmberOneCCLAllreduceGenerator::TAHA::getlocalXPUID(int rank){
    return getglobalXPUID(rank)%m_xpus_per_supernode;
}

void EmberOneCCLAllreduceGenerator::TAHA::configure(){

    allreduce.memSetBacked();
    //m_sendBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );
    m_recvBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );

    //For now assuming for only 2 tiles per XPU
    //TO-DO: Need to talk to Maria on how to implement intra-XPU reduce scatter in case of more than 2 tiles per XPU

    //m_localtileID = getlocaltileID();
    m_localtileID = getlocaltileID(m_rank);
    m_tileneighbor = m_localtileID == 0 ? m_rank+1 : m_rank-1 ;
    //m_supernodeID = getsupernodeID();
    m_supernodeID = getsupernodeID(m_rank);
    //m_localXPUID =  getlocalXPUID();
    m_localXPUID =  getlocalXPUID(m_rank);

    m_supernode_neighbors.resize(m_xpus_per_supernode-1);

    m_intraxpu_cnts.resize(m_tiles_per_xpu);
    m_intraxpu_disps.resize(m_tiles_per_xpu);

    m_intrasupernode_cnts.resize(m_xpus_per_supernode);
    m_intrasupernode_disps.resize(m_xpus_per_supernode);

    //m_requests.resize(4+(3*(m_xpus_per_supernode-1)));
    m_requests_phase1.resize(2);
    m_requests_phase2.resize(m_xpus_per_supernode-1);
    m_requests_phase3.resize(2*(m_xpus_per_supernode-1));
    m_requests_phase4.resize(2);

    int index = 0;
    for(int i=0; i < m_xpus_per_supernode; i++){
        int neighbor = (m_supernodeID*m_xpus_per_supernode*m_tiles_per_xpu) + (i*m_tiles_per_xpu) + m_localtileID;
        if (neighbor != m_rank){
            //m_supernode_neighbors.push_back(neighbor);
            m_supernode_neighbors[index] = neighbor;
	    index += 1;
        }

    }

    //m_intraxpu_cnts.push_back(int(m_count/2));
    m_intraxpu_cnts[0] = int(m_count/2);
    //m_intraxpu_cnts.push_back(m_count - m_intraxpu_cnts[0]);
    m_intraxpu_cnts[1] = (m_count - m_intraxpu_cnts[0]);
    //m_intraxpu_disps.push_back(0);
    m_intraxpu_disps[0] = 0;
    //m_intraxpu_disps.push_back(m_intraxpu_cnts[0]);
    m_intraxpu_disps[1] = (m_intraxpu_cnts[0]);

    if (m_localtileID == 0){

        //int send_cnt = recv_cnt = 0;
        //int send_idx = recv_idx = 0;


	int tmp = 0;
        for (int i=0; i < m_xpus_per_supernode-1;i++){
            //m_intrasupernode_cnts.push_back(int(m_intraxpu_cnts[0] / m_xpus_per_supernode));
            m_intrasupernode_cnts[i] = (int(m_intraxpu_cnts[0] / m_xpus_per_supernode));
            tmp += (int(m_intraxpu_cnts[0] / m_xpus_per_supernode));
        }

        //m_intrasupernode_cnts[m_xpus_per_supernode-1] = ( int(m_intraxpu_cnts[0] - ((int(m_rank/(m_tiles_per_xpu*m_xpus_per_supernode))*m_xpus_per_supernode-1))));
        m_intrasupernode_cnts[m_xpus_per_supernode-1] = m_intraxpu_cnts[0] - tmp;

        m_intrasupernode_disps[0] = m_intraxpu_disps[0];

        for (int i=1; i < m_xpus_per_supernode;i++){
            m_intrasupernode_disps[i] = (m_intrasupernode_disps[i-1]+m_intrasupernode_cnts[i-1]);
        }

    }

    else {

	int tmp = 0;

        for (int i=0; i < m_xpus_per_supernode-1;i++){

            m_intrasupernode_cnts[i] = (int(m_intraxpu_cnts[1] / m_xpus_per_supernode));
	    tmp += (int(m_intraxpu_cnts[1] / m_xpus_per_supernode));

        }

        //m_intrasupernode_cnts[m_xpus_per_supernode-1] = ( int(m_intraxpu_cnts[1] - ((int(m_rank/(m_tiles_per_xpu*m_xpus_per_supernode))*m_xpus_per_supernode-1))));
        m_intrasupernode_cnts[m_xpus_per_supernode-1] = m_intraxpu_cnts[1] - tmp;

        //m_intrasupernode_disps[0] = m_intraxpu_disps[1];
        m_intrasupernode_disps[0] = m_intraxpu_cnts[0];
        //m_intrasupernode_disps[0] = m_intraxpu_disps[0];

        for (int i=1; i < m_xpus_per_supernode;i++){
            m_intrasupernode_disps[i] = (m_intrasupernode_disps[i-1]+m_intrasupernode_cnts[i-1]);
        }

    }

    m_intersupernode_cnts = m_intrasupernode_cnts[m_localXPUID];
    m_intersupernode_disps = m_intrasupernode_disps[m_localXPUID];
    
    /*
    if (m_localtileID != 0) {


    	m_intersupernode_disps += m_intrasupernode_cnts[m_localXPUID];

    
    }
    */

    std::cout << "Debug: Rank: " << m_rank << " m_count : " << m_count << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " int(m_count/2) : " << int(m_count/2) << std::endl;

    //Setting up the communicator world for inter-node allreduce
    m_intersupernode_neighbors.resize(m_supernode_count);

    for (int i=0; i < m_supernode_count; i++){

        m_intersupernode_neighbors[i] = (i*m_xpus_per_supernode*m_tiles_per_xpu) + (m_localXPUID*m_tiles_per_xpu) + m_localtileID;
    }

    m_nicresultBuf = allreduce.memAlloc(allreduce.sizeofDataType(DOUBLE)*m_intersupernode_cnts);


    //m_loopIndex = 0;

    //enQ_commCreate( evQ, GroupWorld, m_intersupernode_neighbors, &m_intersupernodeComm);
    //

    //Debug: dumping m_intraxpu_cnts,m_intraxpu_disps, m_intersupernode_neighbors, m_intersupernode_cnts, m_intersupernode_disps

    for(int i=0; i < 2; i++){
    
    	std::cout << "Debug: Rank: " << m_rank << " m_intraxpu_disps[" << i << "] : " << m_intraxpu_disps[i] << " m_intraxpu_cnts[" << i <<"] : " << m_intraxpu_cnts[i] << std::endl;
    
    }

    for (int i=0; i < m_supernode_count ; i++){
    
    	std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_neighbors[" << i << "] : " << m_intersupernode_neighbors[i] << std::endl;

    }

    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_cnts : " << m_intersupernode_cnts << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << std::endl;

    for (int i=0; i < m_xpus_per_supernode-1; i++){
    
    	std::cout << "Debug: Rank: " << m_rank << " m_supernode_neighbors[" << i << "] : " << m_supernode_neighbors[i] << std::endl; 
    
    }

    for(int i=0; i < m_xpus_per_supernode; i++) {
    
    	std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_cnts[" << i << "] : " << m_intrasupernode_cnts[i] << std::endl;
    	std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_disps[" << i << "] : " << m_intrasupernode_disps[i] << std::endl;
    }










    return;
}

void EmberOneCCLAllreduceGenerator::TAHA::intersupernode_nicallreduce(std::queue<EmberEvent*>& evQ){

    if (m_intersupernode_nicallreduce_algo == "rabenseifner"){

        rabenseifner_nicallreduce(evQ);
    }

}

void EmberOneCCLAllreduceGenerator::TAHA::rabenseifner_nicallreduce(std::queue<EmberEvent*>& evQ){

    /* Implement Rabenseifner algorithm at the inter supernode level
       the MPI ranks involved in this allreduce are stored in m_intersupernode_neighbors
    */

    std::cout << "Debug: Rank: " << m_rank << " Inside rabenseifner_nicallreduce method!" << std::endl;

    int pof2,rem,stages,rank,newrank;

    std::vector<int> cnts, disps;
    //std::vector<MessageRequest> requests_phase1;
    //std::vector<MessageRequest> requests_phase2;

    int val = 2;

    while (val <= m_supernode_count){

        val *= 2;
    }

    pof2 = int(val/2);

    cnts.resize(pof2);
    disps.resize(pof2);
    rem = m_supernode_count - pof2;
    stages = 0;
    rank = getsupernodeID(m_rank);

    if (rank < 2*rem){

        if (rank % 2 == 0){

            newrank = -1;

        }

        else{

            newrank = int(rank/2);
        }
    }

    else {
        newrank = rank - rem;
    }

    if (newrank != -1) {

        //cnts.resize(pof2);
        //disps.resize(pof2);

        for (int i = 0; i < (pof2 - 1); i++)
            cnts[i] = m_intersupernode_cnts / pof2;
        cnts[pof2 - 1] = m_intersupernode_cnts - (m_intersupernode_cnts / pof2) * (pof2 - 1);

        disps[0] = 0;
        for (int i = 1; i < pof2; i++)
            disps[i] = disps[i - 1] + cnts[i - 1];


    }

    requests_phase1.resize(2);
    requests_phase2.resize(2);

    if (newrank != -1) {

    	for (int i=0; i < pof2; i++){
    
    		std::cout << "Debug: Rank: " << m_rank << " cnts[" << i << "] : " << cnts[i] << " disps[" << i <<"] : " << disps[i] << std::endl;
    
    	}

    }


    if (rank < 2*rem){

        if (rank % 2 == 0){

            //allreduce.enQ_send( evQ, sendBuffer, count, dType, dest, tag, *comm );

            allreduce.enQ_send(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank+1],0,GroupWorld);
            
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_send(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank+1],0,GroupWorld);" << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " m_intersupernode_cnts : " << m_intersupernode_cnts << " m_intersupernode_neighbors[rank+1] : " << m_intersupernode_neighbors[rank+1] << std::endl;

        }
        else{
            allreduce.enQ_recv(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank-1],0,GroupWorld);
	    
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_recv(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank-1],0,GroupWorld);" << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " m_intersupernode_cnts : " << m_intersupernode_cnts << " m_intersupernode_neighbors[rank-1] : " << m_intersupernode_neighbors[rank-1] << std::endl;
        }

    }

    if (newrank != -1){

         /* for the reduce-scatter, calculate the count that
         * each process receives and the displacement within
         * the buffer */

        int i, send_idx, recv_idx, last_idx, mask, newdst, dst, send_cnt, recv_cnt;

        mask = 0x1;
        send_idx = 0;
	recv_idx = 0;
        last_idx = pof2;

        while (mask < pof2){


            newdst = newrank ^ mask;
            /* find real rank of dest */
            dst = (newdst < rem) ? newdst * 2 + 1 : newdst + rem;

            send_cnt = 0;
	    recv_cnt = 0;
            if (newrank < newdst) {
                send_idx = recv_idx + pof2 / (mask * 2);
                for (i = send_idx; i < last_idx; i++)
                    send_cnt += cnts[i];
                for (i = recv_idx; i < send_idx; i++)
                    recv_cnt += cnts[i];
            }
            else {
                recv_idx = send_idx + pof2 / (mask * 2);
                for (i = send_idx; i < recv_idx; i++)
                    send_cnt += cnts[i];
                for (i = recv_idx; i < last_idx; i++)
                    recv_cnt += cnts[i];
            }

            allreduce.enQ_irecv(evQ,m_recvBuf + (m_intersupernode_disps + disps[recv_idx])*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase1[0]);
            allreduce.enQ_isend(evQ,m_recvBuf + (m_intersupernode_disps + disps[send_idx])*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase1[1]);
            allreduce.enQ_waitall(evQ,2,&requests_phase1[0],NULL);


	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_irecv(evQ,m_recvBuf + (m_intersupernode_disps + disps[recv_idx])*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase1[0]);" << std::endl; 
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << "disps[recv_idx] = " << disps[recv_idx] << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " recv_cnt : " << recv_cnt << " m_intersupernode_neighbors[dst] : " << m_intersupernode_neighbors[dst] << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + (m_intersupernode_disps + disps[send_idx])*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase1[1]);" << std::endl; 
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << "disps[send_idx] = " << disps[send_idx] << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " send_cnt : " << send_cnt << " m_intersupernode_neighbors[dst] : " << m_intersupernode_neighbors[dst] << std::endl;


            /* update send_idx for next iteration */
            send_idx = recv_idx;
            mask <<= 1;

            /* update last_idx, but not in last iteration
             * because the value is needed in the allgather
             * step below. */
            if (mask < pof2)
                last_idx = recv_idx + pof2 / mask;



        }

        /* Now do the allgather */

        mask >>= 1;
        while (mask > 0) {
            newdst = newrank ^ mask;
            /* find real rank of dest */
            dst = (newdst < rem) ? newdst * 2 + 1 : newdst + rem;

            send_cnt = 0;
	    recv_cnt = 0;
            if (newrank < newdst) {
                /* update last_idx except on first iteration */
                if (mask != pof2 / 2)
                    last_idx = last_idx + pof2 / (mask * 2);

                recv_idx = send_idx + pof2 / (mask * 2);
                for (i = send_idx; i < recv_idx; i++)
                    send_cnt += cnts[i];
                for (i = recv_idx; i < last_idx; i++)
                    recv_cnt += cnts[i];
            }
            else {
                recv_idx = send_idx - pof2 / (mask * 2);
                for (i = send_idx; i < last_idx; i++)
                    send_cnt += cnts[i];
                for (i = recv_idx; i < send_idx; i++)
                    recv_cnt += cnts[i];
            }


            allreduce.enQ_irecv(evQ,m_recvBuf + (disps[recv_idx] + m_intersupernode_disps)*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase2[0]);
            allreduce.enQ_isend(evQ,m_recvBuf + (disps[send_idx] + m_intersupernode_disps)*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase2[1]);
            allreduce.enQ_waitall(evQ,2,&requests_phase2[0],NULL);


	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_irecv(evQ,m_recvBuf + (disps[recv_idx] + m_intersupernode_disps)*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase2[0]);" << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << "disps[recv_idx] = " << disps[recv_idx] << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " recv_cnt : " << recv_cnt << " m_intersupernode_neighbors[dst] : " << m_intersupernode_neighbors[dst] << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + (disps[send_idx] + m_intersupernode_disps)*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_intersupernode_neighbors[dst],0,GroupWorld,&requests_phase2[1]);" << std::endl;
	    std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << "disps[send_idx] = " << disps[send_idx] << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " send_cnt : " << send_cnt << " m_intersupernode_neighbors[dst] : " << m_intersupernode_neighbors[dst] << std::endl;


            if (newrank > newdst)
                send_idx = recv_idx;

            mask >>= 1;
        }

    }

    /* In the non-power-of-two case, all odd-numbered
     * processes of rank < 2*rem send the result to
     * (rank-1), the ranks who didn't participate above. */
    if (rank < 2 * rem) {
        if (rank % 2) { /* odd */

            allreduce.enQ_send(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank-1],0,GroupWorld);
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_send(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank-1],0,GroupWorld);" << std::endl;
            std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " m_intersupernode_cnts : " << m_intersupernode_cnts << " m_intersupernode_neighbors[rank-1] : " << m_intersupernode_neighbors[rank-1] << std::endl;

        }

        else {

            allreduce.enQ_recv(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank+1],0,GroupWorld);
	    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_recv(evQ,m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE),m_intersupernode_cnts,DOUBLE,m_intersupernode_neighbors[rank+1],0,GroupWorld);" << std::endl;
            std::cout << "Debug: Rank: " << m_rank << " m_intersupernode_disps : " << m_intersupernode_disps << " allreduce.sizeofDataType(DOUBLE) : " << allreduce.sizeofDataType(DOUBLE) << " m_intersupernode_cnts : " << m_intersupernode_cnts << " m_intersupernode_neighbors[rank+1] : " << m_intersupernode_neighbors[rank+1] << std::endl;

        }

    }

    std::cout << "Debug: Rank: " << m_rank << " Exiting rabenseifner allreduce method!" << std::endl;


}

void EmberOneCCLAllreduceGenerator::TAHA::generate(std::queue<EmberEvent*>& evQ){

    /*1. Inter-XPU Allreduce
    For now, assuming that there are only two tiles per XPU.
    Tile 0 receives the first half of the msg-size and sends the second half to Tile 1
    Tile 1 receives the second half of the msg-size and sends the first half to Tile 0
    */

    int send_idx = 0;
    int recv_idx = 0;
    int send_cnt = 0;
    int recv_cnt = 0;

    if (m_rank < m_tileneighbor) {

        recv_idx = m_intraxpu_disps[0];
        recv_cnt = m_intraxpu_cnts[0];
        send_idx = m_intraxpu_disps[1];
        send_cnt = m_intraxpu_cnts[1];
      

    }

    else {

        recv_idx = m_intraxpu_disps[1];
        recv_cnt = m_intraxpu_cnts[1];
        send_idx = m_intraxpu_disps[0];
        send_cnt = m_intraxpu_cnts[0];

    }


    allreduce.enQ_irecv(evQ,m_recvBuf + recv_idx*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase1[0]);
    allreduce.enQ_isend(evQ,m_recvBuf + send_idx*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase1[1]);
    allreduce.enQ_waitall(evQ,2,&m_requests_phase1[0],NULL);


    /*
    std::cout << "Debug: Rank: " << m_rank << " Phase 1: Inter tile reduce scatter begin!" << std::endl; 

    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_irecv(evQ,m_recvBuf + recv_idx*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase1[0]);" << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " recv_idx: " << recv_idx << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << "recv_cnt: " <<recv_cnt << " m_tileneighbor: " << m_tileneighbor << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + send_idx*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase1[1]);" << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " send_idx: " << send_idx << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << "send_cnt: " <<send_cnt << " m_tileneighbor: " << m_tileneighbor << std::endl;
    std::cout << "allreduce.enQ_waitall(evQ,2,&m_requests_phase1[0],NULL);" << std::endl;


    std::cout << "Debug: Rank: " << m_rank << " Phase 1: Inter tile reduce scatter end!" << std::endl; 
    */
    //TO-DO: Add a compute model for Reduction Operation, e.g..,SUM.
    //allreduce.enQ_compute(reductionsum_compute_ns);

    /*2. Intra-supernode Multi Incast.
    In this step, each tile in a supernode receives a specific part of the data from all the corresponding 
    TO-DO: Need to talk to Maria on the actual implementation. For now, implementing a non-blocking send and a blocking receive.
    */

    //std::cout << "Debug: Rank: " << m_rank << " Phase 2: Intra-supernode/Inter-XPU reduce scatter begin!" << std::endl; 

    for (int i=0; i < m_xpus_per_supernode-1;i++){

        int dst = m_supernode_neighbors[i];

        int local_dst = getlocalXPUID(dst);

        

        allreduce.enQ_isend(evQ,m_recvBuf + m_intrasupernode_disps[local_dst]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[local_dst],DOUBLE,dst,0,GroupWorld,&m_requests_phase2[i]);
	//std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + m_intrasupernode_disps[local_dst]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[local_dst],DOUBLE,dst,0,GroupWorld,&m_requests_phase2[i]);" << std::endl;
	//std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_disps[local_dst]: " << m_intrasupernode_disps[local_dst] << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << " m_intrasupernode_cnts[local_dst]: " << m_intrasupernode_cnts[local_dst] << " dst: " << dst << std::endl;


    }

    for (int i=0; i < m_xpus_per_supernode-1;i++){

        int src = m_supernode_neighbors[i];

        allreduce.enQ_recv(evQ,m_recvBuf + m_intrasupernode_disps[m_localXPUID]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[m_localXPUID],DOUBLE,src,0,GroupWorld);
	//std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_recv(evQ,m_recvBuf + m_intrasupernode_disps[m_localXPUID]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[m_localXPUID],DOUBLE,src,0,GroupWorld);" << std::endl;
	//std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_disps[m_localXPUID]: " << m_intrasupernode_disps[m_localXPUID] << "allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << " m_intrasupernode_cnts[m_localXPUID]: " << m_intrasupernode_cnts[m_localXPUID] << " src: " << src << std::endl;
        //allreduce.enQ_compute(reductionsum_compute_ns);
        
    }

    //Wait until you send to all corresponding tiles before starting Inter node
    allreduce.enQ_waitall(evQ,m_xpus_per_supernode-1,&m_requests_phase2[0],NULL);

    //std::cout << "Debug: Rank: " << m_rank << " Phase 2: Intra-supernode/Inter-XPU reduce scatter end!" << std::endl; 
    /*3. In this stage, we perform inter supernode allreduce. Each tile,XPU performs an allreduce operation w.r.t corresponding tile,XPU IDs among all the supernodes. 
    TO-DO: Need to talk to Maria on what type of allreduce algorithm is used at the internode level
    */

    intersupernode_nicallreduce(evQ);

    allreduce.enQ_barrier( evQ, GroupWorld );


    /*
    if (loopIndex = 0) {

    	allreduce.enQ_commCreate( evQ, GroupWorld, m_intersupernode_neighbors, &m_intersupernodeComm);
	//m_loopIndex++;
    
    }

    //std::cout << "Calling Firefly Allreduce!" << std::endl;

    //allreduce.enQ_allreduce( evQ, m_recvBuf + m_intersupernode_disps*allreduce.sizeofDataType(DOUBLE), m_nicresultBuf, m_intersupernode_cnts, DOUBLE, Hermes::MP::SUM, m_intersupernodeComm );

    //std::cout << "Finished Firefly Allreduce!" << std::endl;

    */

    /*4. In the final stage, we perform Intrasupernode Allgather
    */

    //Putting a barrier to make sure all intersupernode allreduces are completed
    //allreduce.enQ_barrier( evQ, GroupWorld );

    //Allgather is functionally complement to the reduce-scatter stages we performed at the beginning
    //As we are not doing any reduction operation, using Non-blocking Sends/Receives would be op
    
    //std::cout << "Debug: Rank: " << m_rank << " Phase 3: Intra-supernode/Inter-XPU allgather begin!" << std::endl; 
    
    //std::cout << "Starting Intra-supernode Allgather (Across XPUs) ! " << std::endl;
    int offset = 2+m_xpus_per_supernode-1;
    //Across XPUs within supernode
    for (int i=0; i < m_xpus_per_supernode-1;i++){

        int src = m_supernode_neighbors[i];
        int local_dst = getlocalXPUID(src);

        allreduce.enQ_isend(evQ,m_recvBuf + m_intrasupernode_disps[m_localXPUID]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[m_localXPUID],DOUBLE,src,0,GroupWorld,&m_requests_phase3[(2*i)]);
        allreduce.enQ_irecv(evQ,m_recvBuf + m_intrasupernode_disps[local_dst]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[local_dst],DOUBLE,src,0,GroupWorld,&m_requests_phase3[(2*i+1)]);
        //allreduce.enQ_compute(reductionsum_compute_ns);
	/*
	std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + m_intrasupernode_disps[m_localXPUID]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[m_localXPUID],DOUBLE,src,0,GroupWorld,&m_requests_phase3[(2*i)]);" << std::endl;
	std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_disps[m_localXPUID]: " << m_intrasupernode_disps[m_localXPUID] << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << " m_intrasupernode_cnts[m_localXPUID]: " << m_intrasupernode_cnts[m_localXPUID] << "  src: " << src << std::endl;
	std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_irecv(evQ,m_recvBuf + m_intrasupernode_disps[local_dst]*allreduce.sizeofDataType(DOUBLE),m_intrasupernode_cnts[local_dst],DOUBLE,src,0,GroupWorld,&m_requests_phase3[(2*i+1)]);" << std::endl;
	std::cout << "Debug: Rank: " << m_rank << " m_intrasupernode_disps[local_dst]: " << m_intrasupernode_disps[local_dst] << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << " m_intrasupernode_cnts[local_dst]: " << m_intrasupernode_cnts[local_dst] << "  src: " << src << std::endl;
	*/
        
    }

    allreduce.enQ_waitall(evQ,(2*(m_xpus_per_supernode-1)),&m_requests_phase3[0],NULL);

    //std::cout << "Finished Intra-supernode Allgather (Across XPUs) ! " << std::endl;
    //std::cout << "Debug: Rank: " << m_rank << " Phase 3: Intra-supernode/Inter-XPU allgather end!" << std::endl; 

    /*
    Tile 0 sends the first half of the msg-size and receives the second half to Tile 1
    Tile 1 sends the second half of the msg-size and receives the first half to Tile 0
    */

    //int send_idx = recv_idx = 0;
    send_idx = 0;
    recv_idx = 0;
    //int send_cnt = recv_cnt = 0;
    send_cnt = 0;
    recv_cnt = 0;

    if (m_rank < m_tileneighbor) {

        recv_idx = m_intraxpu_disps[1];
        recv_cnt = m_intraxpu_cnts[1];
        send_idx = m_intraxpu_disps[0];
        send_cnt = m_intraxpu_cnts[0];
      

    }

    else {

        recv_idx = m_intraxpu_disps[0];
        recv_cnt = m_intraxpu_cnts[0];
        send_idx = m_intraxpu_disps[1];
        send_cnt = m_intraxpu_cnts[1];

    }

    //std::cout << "Debug: Rank: " << m_rank << " Phase 4: Intra-XPU/Inter-tile allgather begin!" << std::endl; 
    //std::cout << "Starting Intra-supernode Allgather (Across tiles) ! " << std::endl;

    offset = 2+(3*(m_xpus_per_supernode-1));

    allreduce.enQ_irecv(evQ,m_recvBuf + recv_idx*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase4[0]);
    allreduce.enQ_isend(evQ,m_recvBuf + send_idx*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase4[1]);
    allreduce.enQ_waitall(evQ,2,&m_requests_phase4[0],NULL);

    /*
    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_irecv(evQ,m_recvBuf + recv_idx*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase4[0]);" << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " recv_idx: " << recv_idx << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << "recv_cnt: " << recv_cnt << " m_tileneighbor: " << m_tileneighbor << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " allreduce.enQ_isend(evQ,m_recvBuf + send_idx*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,m_tileneighbor,0,GroupWorld,&m_requests_phase4[1]);" << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " send_idx: " << recv_idx << " allreduce.sizeofDataType(DOUBLE): " << allreduce.sizeofDataType(DOUBLE) << "send_cnt: " << send_cnt << " m_tileneighbor: " << m_tileneighbor << std::endl;
    std::cout << "Finished Intra-supernode Allgather (Across tiles) ! " << std::endl;
    std::cout << "Debug: Rank: " << m_rank << " Phase 4: Intra-XPU/Inter-tile allgather end!" << std::endl; 
    */

    loopIndex++;

    /*
    if (loopIndex == iterations)
    {
    	std::cout << "Starting commDestroy ! " << std::endl;
    	allreduce.enQ_commDestroy( evQ, m_intersupernodeComm);
    	std::cout << "Finished commDestroy ! " << std::endl;
    }
    */



}

EmberOneCCLAllreduceGenerator::TAHA::~TAHA(){

    allreduce.memFree(m_recvBuf);
    //allreduce.memFree(m_sendBuf);
}
