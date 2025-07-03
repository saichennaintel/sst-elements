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

//OneCCL Allreduce Algorithm: Rabenseifner
//SST implementation is derived from the implementation in https://github.com/oneapi-src/oneCCL/blob/master/src/coll/algorithms/allreduce/allreduce.cpp

#include <sst_config.h>
#include "../motifs/emberonecclallreduce.h"

using namespace SST::Ember;
using namespace SST::Hermes::MP;

EmberOneCCLAllreduceGenerator::Rabenseifner::Rabenseifner(EmberOneCCLAllreduceGenerator& parent, Params& params, int iterations) : allreduce(parent) , iterations(iterations) , loopIndex(0) {

    
    //m_world = allreduce.size();
    m_world = allreduce.size();
    m_count = (uint32_t) params.find("arg.count", 1);
    m_rank = allreduce.rank();

    m_verbose = (bool) params.find("arg.verbose",false);




}

int EmberOneCCLAllreduceGenerator::Rabenseifner::getpof2(){



    int val = 2;

    while (val <= m_world){

        val *= 2;
    }

    return int(val/2);
}

void EmberOneCCLAllreduceGenerator::Rabenseifner::configure(){


    m_pof2 = getpof2();
    m_rem = m_world - m_pof2;
    m_stages = 0;

    allreduce.memSetBacked();
    m_sendBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );
    m_recvBuf =   allreduce.memAlloc( m_count * allreduce.sizeofDataType(DOUBLE) );

    if (m_rank < 2*m_rem){

        if (m_rank % 2 == 0){

            m_newrank = -1;

        }

        else{

            m_newrank = int(m_rank/2);
        }
    }

    else {
        m_newrank = m_rank - m_rem;
    }

    if (m_newrank != -1) {

        m_cnts.resize(m_pof2);
        m_disps.resize(m_pof2);

        for (int i = 0; i < (m_pof2 - 1); i++)
            m_cnts[i] = m_count / m_pof2;
        m_cnts[m_pof2 - 1] = m_count - (m_count / m_pof2) * (m_pof2 - 1);

        m_disps[0] = 0;
        for (int i = 1; i < m_pof2; i++)
            m_disps[i] = m_disps[i - 1] + m_cnts[i - 1];


    }

    m_requests.resize(2);

}



void EmberOneCCLAllreduceGenerator::Rabenseifner::generate(std::queue<EmberEvent*>& evQ){


    if (m_rank < 2*m_rem){

        if (m_rank % 2 == 0){

            //allreduce.enQ_send( evQ, sendBuffer, count, dType, dest, tag, *comm );

            allreduce.enQ_send(evQ,m_recvBuf,m_count,DOUBLE,m_rank+1,0,GroupWorld);


            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Stage: Non-power of 2 Init Comm op: Send m_count = " << m_count << " Destination Rank: " << m_rank+1 << std::endl;
            }

        }
        else{

            allreduce.enQ_recv(evQ,m_recvBuf,m_count,DOUBLE,m_rank-1,0,GroupWorld);

            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Stage: Non-power of 2 Init Comm op: Recv m_count = " << m_count << " Destination Rank: " << m_rank-1 << std::endl;
            }
        }

    }

    if (m_newrank != -1){

         /* for the reduce-scatter, calculate the count that
         * each process receives and the displacement within
         * the buffer */

        int i, send_idx, recv_idx, last_idx, mask, newdst, dst, send_cnt, recv_cnt;

        mask = 0x1;
        send_idx = recv_idx = 0;
        last_idx = m_pof2;


        int stage = 1;


        while (mask < m_pof2){


            newdst = m_newrank ^ mask;
            /* find real rank of dest */
            dst = (newdst < m_rem) ? newdst * 2 + 1 : newdst + m_rem;

            send_cnt = recv_cnt = 0;
            if (m_newrank < newdst) {
                send_idx = recv_idx + m_pof2 / (mask * 2);
                for (i = send_idx; i < last_idx; i++)
                    send_cnt += m_cnts[i];
                for (i = recv_idx; i < send_idx; i++)
                    recv_cnt += m_cnts[i];
            }
            else {
                recv_idx = send_idx + m_pof2 / (mask * 2);
                for (i = send_idx; i < recv_idx; i++)
                    send_cnt += m_cnts[i];
                for (i = recv_idx; i < last_idx; i++)
                    recv_cnt += m_cnts[i];
            }

            allreduce.enQ_irecv(evQ,m_recvBuf + m_disps[recv_idx]*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,dst,0,GroupWorld,&m_requests[0]);
            allreduce.enQ_isend(evQ,m_recvBuf + m_disps[send_idx]*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,dst,0,GroupWorld,&m_requests[1]);
            allreduce.enQ_waitall(evQ,2,&m_requests[0],NULL);


            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Reduce Scatter Phase: Stage: " << stage << "  Comm op: IRecv recv_count = " << recv_cnt << " Destination Rank: " << dst << std::endl;
                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Reduce Scatter Phase: Stage: " << stage << "  Comm op: ISend send_count = " << send_cnt << " Destination Rank: " << dst << std::endl;
                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Reduce Scatter Phase: Stage: " << stage << "  Comm op: WaitAll " << std::endl;

                stage += 1;

            }


            /* update send_idx for next iteration */
            send_idx = recv_idx;
            mask <<= 1;

            /* update last_idx, but not in last iteration
             * because the value is needed in the allgather
             * step below. */
            if (mask < m_pof2)
                last_idx = recv_idx + m_pof2 / mask;



        }

        stage = 1;

        /* Now do the allgather */

        mask >>= 1;
        while (mask > 0) {
            newdst = m_newrank ^ mask;
            /* find real rank of dest */
            dst = (newdst < m_rem) ? newdst * 2 + 1 : newdst + m_rem;

            send_cnt = recv_cnt = 0;
            if (m_newrank < newdst) {
                /* update last_idx except on first iteration */
                if (mask != m_pof2 / 2)
                    last_idx = last_idx + m_pof2 / (mask * 2);

                recv_idx = send_idx + m_pof2 / (mask * 2);
                for (i = send_idx; i < recv_idx; i++)
                    send_cnt += m_cnts[i];
                for (i = recv_idx; i < last_idx; i++)
                    recv_cnt += m_cnts[i];
            }
            else {
                recv_idx = send_idx - m_pof2 / (mask * 2);
                for (i = send_idx; i < last_idx; i++)
                    send_cnt += m_cnts[i];
                for (i = recv_idx; i < send_idx; i++)
                    recv_cnt += m_cnts[i];
            }


            allreduce.enQ_irecv(evQ,m_recvBuf + m_disps[recv_idx]*allreduce.sizeofDataType(DOUBLE),recv_cnt,DOUBLE,dst,0,GroupWorld,&m_requests[0]);
            allreduce.enQ_isend(evQ,m_recvBuf + m_disps[send_idx]*allreduce.sizeofDataType(DOUBLE),send_cnt,DOUBLE,dst,0,GroupWorld,&m_requests[1]);
            allreduce.enQ_waitall(evQ,2,&m_requests[0],NULL);

            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " AllGather Phase: Stage: " << stage << "  Comm op: IRecv recv_count = " << recv_cnt << " Destination Rank: " << dst << std::endl;
                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " AllGather Phase: Stage: " << stage << "  Comm op: ISend send_count = " << send_cnt << " Destination Rank: " << dst << std::endl;
                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " AllGather Phase: Stage: " << stage << "  Comm op: WaitAll " << std::endl;

                stage += 1;

            }

            if (m_newrank > newdst)
                send_idx = recv_idx;

            mask >>= 1;
        }

    }

    /* In the non-power-of-two case, all odd-numbered
     * processes of rank < 2*rem send the result to
     * (rank-1), the ranks who didn't participate above. */
    if (m_rank < 2 * m_rem) {
        if (m_rank % 2) { /* odd */

            allreduce.enQ_send(evQ,m_recvBuf,m_count,DOUBLE,m_rank-1,0,GroupWorld);

            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Stage: Non-power of 2 Fini Comm op: Send m_count = " << m_count << " Destination Rank: " << m_rank-1 << std::endl;
            }

        }

        else {

            allreduce.enQ_recv(evQ,m_recvBuf,m_count,DOUBLE,m_rank+1,0,GroupWorld);

            if (m_verbose) {

                std::cout << "Rabenseifner verbose: " << "Rank: " << m_rank << " Stage: Non-power of 2 Fini Comm op: Recv m_count = " << m_count << " Destination Rank: " << m_rank+1 << std::endl;
            }

        }

    }

}

EmberOneCCLAllreduceGenerator::Rabenseifner::~Rabenseifner(){

    allreduce.memFree(m_recvBuf);
    allreduce.memFree(m_sendBuf);
}
