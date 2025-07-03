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

//OneCCL Allreduce Algorithm: Ring
//SST implementation is derived from the implementation in https://github.com/oneapi-src/oneCCL/blob/master/src/coll/algorithms/allreduce/allreduce.cpp

#include <sst_config.h>
#include "../motifs/emberonecclallreduce.h"
#include <vector>
using namespace SST::Ember;
using namespace SST::Hermes::MP;

EmberOneCCLAllreduceGenerator::Ring::Ring(EmberOneCCLAllreduceGenerator& parent, Params& params, int iterations, bool inplace) : allreduce(parent) , iterations(iterations) , loopIndex(0), inplace(inplace) {

    
    //std::cout << "Debug: Inside Ring constructor!!" << std::endl;

    m_world = allreduce.size();
    m_count = (uint32_t) params.find("arg.count", 1);
    m_rank = allreduce.rank();
    //m_minchunksize = params.find<int>("arg.minchunksize",2048);
    //m_minchunksize = params.find<int>("arg.minchunksize",2048);
    //m_minchunksize = params.find<int>("arg.minchunksize",2097152);
    m_minchunksize = params.find<int>("arg.minchunksize",65536);
    //m_minchunksize = params.find<int>("arg.minchunksize",0);
    m_chunkcount = (size_t) params.find<int>("arg.chunkcount",1);
    m_memBW = params.find<double>("arg.memBW",100);

    if (m_minchunksize == 8 && m_chunkcount == 1) {
 
        m_dochunks = true;

    }

    m_mainblock_count = m_count / m_world ;
    m_lastblock_count = m_mainblock_count + m_count % m_world;

    //std::cout << "Rank: " << m_rank << " m_mainblock_count: " << m_mainblock_count << " m_lastblock_count: " << m_lastblock_count << std::endl;

    for (int i = 0 ; i < m_world ; i++) {

        m_recv_counts.push_back(m_mainblock_count) ;

    }

    //m_recv_counts((int)m_world, (size_t) m_mainblock_count);

    if (m_count % m_world){
        m_recv_counts[m_world-1] = m_lastblock_count;
    }




}


void EmberOneCCLAllreduceGenerator::Ring::configure(){

    //std::cout << "Debug: Inside Ring configure() !!" << std::endl;

    allreduce.memSetBacked();
    m_sendBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );
    m_recvBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );

    m_requests.resize(4);

}



void EmberOneCCLAllreduceGenerator::Ring::generate(std::queue<EmberEvent*>& evQ){

    ring_reduce_scatter(evQ);
    allreduce.enQ_barrier( evQ, GroupWorld );
    ring_allgatherv(evQ);
    allreduce.enQ_barrier( evQ, GroupWorld );


}

double EmberOneCCLAllreduceGenerator::Ring::getCopyTimens(size_t count){

    //Assuming memBW is specified in GB/s

    /*
    size_t bytes = m_count * allreduce.sizeofDataType(DOUBLE);

    return (double) bytes/m_memBW;
    */

    return 0;

}

double EmberOneCCLAllreduceGenerator::Ring::getReduceTimens(size_t count){


    //Reduction time (ns) = No of bytes * FLOPs per bytes of reduction * (1/GFLOPSpersec)
    //double reduction_time_ns = count * m_procFlops * m_procFreq;
    
    //For now
    return 0 ;


}

void EmberOneCCLAllreduceGenerator::Ring::ring_reduce_scatter(std::queue<EmberEvent*>& evQ){


    int src = (m_world + m_rank - 1) % m_world;
    int dst = (m_world + m_rank + 1) % m_world;

    size_t bytes = m_count * allreduce.sizeofDataType(DOUBLE);

    size_t tmp_count = (size_t) (bytes >= m_minchunksize && (size_t)m_count >= m_chunkcount && m_count >= m_world) ? m_chunkcount : 1 ;

    m_chunkcount = tmp_count;

    while ((m_chunkcount > 1) && (bytes/ (m_world * m_chunkcount)) < m_minchunksize) {
        m_chunkcount--;
    }

    if (m_chunkcount == 0) {
        //std::cout << "Debug: Ring::ring_reduce_scatter method: Unexpected chunk count!" << std::endl;
        m_chunkcount = 1;
    }

    //Debug
    std::string in_place = inplace ? "in-place" : "out-of-place";
    //std::cout << "Debug: Ring::ring_reduce_scatter method: " << in_place << std::endl;

    if (m_world == 1) {

        if (!inplace) {
            double time = getCopyTimens(m_count);
            allreduce.enQ_compute(evQ,time);
            allreduce.enQ_barrier(evQ,GroupWorld);
        }
        return;

    }

    int block_idx = (m_rank + m_world - 1) % m_world;
    int send_block_idx, recv_block_idx;
    
    //Need to check w/ Nalini if we need this
    /*
    if (inplace){

        allreduce.enQ_compute(evQ,bufferalloctimens);

    }
    */

    size_t send_block_size, recv_block_size;
    size_t send_block_offset, recv_block_offset;

    size_t send_main_chunk_size, send_last_chunk_size;
    size_t recv_main_chunk_size, recv_last_chunk_size;

    size_t send_chunk_size, recv_chunk_size = 0, reduce_chunk_size;
    size_t send_chunk_offset, recv_chunk_offset = 0, reduce_chunk_offset;

    /* if chunk_count > 1 then make reduction with 1 chunk delay to get comp/comp overlapping */
    bool use_prev = (m_chunkcount > 1) ? true : false;
    size_t prev_recv_chunk_size, prev_recv_chunk_offset;


    for (int idx = 0; idx < (m_world - 1); idx++) {

        send_block_idx = block_idx;
        recv_block_idx = (m_world + block_idx - 1) % m_world;

        send_block_size = ( send_block_idx == (m_world -1) ) ? m_lastblock_count : m_mainblock_count ;
        recv_block_size = ( recv_block_idx == (m_world -1) ) ? m_lastblock_count : m_mainblock_count ;

        send_block_offset = m_mainblock_count * send_block_idx * allreduce.sizeofDataType(DOUBLE);
        recv_block_offset = m_mainblock_count * recv_block_idx * allreduce.sizeofDataType(DOUBLE);

        send_main_chunk_size = send_block_size / m_chunkcount ;
        send_last_chunk_size = send_main_chunk_size + send_block_size % m_chunkcount ;

        recv_main_chunk_size = recv_block_size / m_chunkcount ;
        recv_last_chunk_size = recv_main_chunk_size + recv_block_size % m_chunkcount ;

        for (size_t chunk_idx = 0 ; chunk_idx < m_chunkcount ; chunk_idx++ ) {

            prev_recv_chunk_size = recv_chunk_size ;
            send_chunk_size = ( chunk_idx == (m_chunkcount - 1)) ? send_last_chunk_size : send_main_chunk_size ;
            recv_chunk_size = ( chunk_idx == (m_chunkcount - 1)) ? recv_last_chunk_size : recv_main_chunk_size ;

            reduce_chunk_size = (use_prev) ? prev_recv_chunk_offset : recv_chunk_offset;

            prev_recv_chunk_offset = recv_chunk_offset;
            send_chunk_offset = send_block_offset + send_main_chunk_size * chunk_idx * allreduce.sizeofDataType(DOUBLE);
            recv_chunk_offset = recv_block_offset + recv_main_chunk_size * chunk_idx * allreduce.sizeofDataType(DOUBLE);
            reduce_chunk_offset = (use_prev) ? prev_recv_chunk_offset : recv_chunk_offset;

            if (inplace) {

                allreduce.enQ_isend(evQ,m_recvBuf + send_chunk_offset, send_chunk_size,DOUBLE,dst,0,GroupWorld,&m_requests[0]);

                if (!use_prev){

                    allreduce.enQ_irecv(evQ,m_recvBuf + reduce_chunk_offset,recv_chunk_size,DOUBLE,src,0,GroupWorld,&m_requests[1]);
                    allreduce.enQ_compute(evQ,getReduceTimens(recv_chunk_size));


                }

                else {


                    allreduce.enQ_irecv(evQ,m_recvBuf + recv_chunk_offset,recv_chunk_size,DOUBLE,src,0,GroupWorld,&m_requests[1]);

                    if (idx + chunk_idx > 0) {

                        //Do the reduce with one-chunk delay

                        allreduce.enQ_compute(evQ,getReduceTimens(reduce_chunk_size));


                    }

                    if ((idx == m_world - 2) && (chunk_idx == m_chunkcount - 1)) {

                        allreduce.enQ_barrier( evQ, GroupWorld );

                        allreduce.enQ_compute(evQ,getReduceTimens(recv_chunk_size));



                    }






                }


            }

            else {

                if (idx == 0) {

                    allreduce.enQ_isend(evQ,m_sendBuf + send_chunk_offset, send_chunk_size,DOUBLE,dst,0,GroupWorld,&m_requests[0]);

		    //std::cout << "Rank: " << m_rank << " Phase: Reduce Scatter " << " Comm Stage: " << idx << " Isend: Chunk iteration/index: " << chunk_idx << " Send chunk size: " << send_chunk_size << " dest rank: " << dst << std::endl; 

                }

                else {

                    allreduce.enQ_isend(evQ,m_recvBuf + send_chunk_offset, send_chunk_size,DOUBLE,dst,0,GroupWorld,&m_requests[0]);
		    //std::cout << "Rank: " << m_rank << " Phase: Reduce Scatter " << " Comm Stage: " << idx << " Isend: Chunk iteration/index: " << chunk_idx << " Send chunk size: " << send_chunk_size << " dest rank: " << dst << std::endl; 

                }

                if (!use_prev){

                    allreduce.enQ_irecv(evQ,m_sendBuf + reduce_chunk_offset,recv_chunk_size,DOUBLE,src,0,GroupWorld,&m_requests[1]);
		    //std::cout << "Rank: " << m_rank << " Phase: Reduce Scatter " << " Comm Stage: " << idx << " Irecv: Chunk iteration/index: " << chunk_idx << " Recv chunk size: " << recv_chunk_size << " src rank: " << src << std::endl; 
                    //allreduce.enQ_compute(evQ,reducecomptimens);
                    allreduce.enQ_compute(evQ,getReduceTimens(recv_chunk_size));


                }

                else {


                    allreduce.enQ_irecv(evQ,m_recvBuf + recv_chunk_offset,recv_chunk_size,DOUBLE,src,0,GroupWorld,&m_requests[1]);
		    //std::cout << "Rank: " << m_rank << " Phase: Reduce Scatter " << " Comm Stage: " << idx << " Irecv: Chunk iteration/index: " << chunk_idx << " Recv chunk size: " << recv_chunk_size << " src rank: " << src << std::endl; 

                    if (idx + chunk_idx > 0) {

                        //Do the reduce with one-chunk delay

                        //allreduce.enQ_compute(evQ,reducetimecomptimens);
                        allreduce.enQ_compute(evQ,getReduceTimens(reduce_chunk_size));


                    }

                    if ((idx == m_world - 2) && (chunk_idx == m_chunkcount - 1)) {

                        allreduce.enQ_barrier( evQ, GroupWorld );

                        //allreduce.enQ_compute(evQ,reducetimecomptimens);
                        allreduce.enQ_compute(evQ,getReduceTimens(recv_chunk_size));



                    }






                }



            }

            allreduce.enQ_waitall(evQ,2,&m_requests[0],NULL);


        }

    }    

    /*TO-DO*/


}

void EmberOneCCLAllreduceGenerator::Ring::ring_allgatherv(std::queue<EmberEvent*>& evQ) {

    std::vector<int> offsets(m_world,0);

    offsets[0] = 0;

    for (int rank_idx = 1; rank_idx < m_world; ++rank_idx){

        offsets[rank_idx] = offsets[rank_idx-1] + m_recv_counts[rank_idx - 1]*allreduce.sizeofDataType(DOUBLE) ;

    }

    int src = (m_world + m_rank - 1) % m_world;
    int dst = (m_world + m_rank + 1) % m_world;

    size_t block_idx = m_rank; // start send with 'rank' block and recv with 'rank-1' block and move blocks left
    size_t send_block_idx, recv_block_idx;
    size_t send_block_count, recv_block_count;
    size_t send_block_offset, recv_block_offset;

    for (int idx = 0; idx < (m_world - 1); idx++) {
        send_block_idx = block_idx;
        recv_block_idx = (m_world + block_idx - 1) % m_world;
        send_block_count = m_recv_counts[send_block_idx];
        recv_block_count = m_recv_counts[recv_block_idx];
        send_block_offset = offsets[send_block_idx];
        recv_block_offset = offsets[recv_block_idx];


        allreduce.enQ_isend(evQ,m_recvBuf + (send_block_count)*allreduce.sizeofDataType(DOUBLE),send_block_count,DOUBLE,dst,0,GroupWorld,&m_requests[2]);
	
	//std::cout << "Rank: " << m_rank << " Phase: Allgather " << " Comm Stage: " << idx << " Isend: " << " send block count: " << send_block_count << " dst rank: " << dst << std::endl; 
        
	allreduce.enQ_irecv(evQ,m_recvBuf + (recv_block_count)*allreduce.sizeofDataType(DOUBLE),recv_block_count,DOUBLE,src,0,GroupWorld,&m_requests[3]);

	//std::cout << "Rank: " << m_rank << " Phase: Allgather " << " Comm Stage: " << idx << " Irecv: " << " recv block count: " << recv_block_count << " src rank: " << src << std::endl; 


        allreduce.enQ_waitall(evQ,2,&m_requests[2],NULL);


        block_idx = (m_world + block_idx - 1) % m_world; // move left
    }



}


EmberOneCCLAllreduceGenerator::Ring::~Ring(){

    allreduce.memFree(m_recvBuf);
    allreduce.memFree(m_sendBuf);

}
