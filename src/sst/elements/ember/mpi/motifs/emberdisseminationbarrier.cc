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


#include <sst_config.h>
#include "emberdisseminationbarrier.h"

using namespace SST::Ember;

EmberOneCCLDisseminationBarrierGenerator::EmberOneCCLDisseminationBarrierGenerator(SST::ComponentId_t id, Params& params) :
	EmberMessagePassingGenerator(id, params, "OneCCLDisseminationBarrier"),
    m_loopIndex(0),
    m_addr(1,NULL)
{
	m_iterations = (uint32_t) params.find("arg.iterations", 1);
    m_compute    = (uint32_t) params.find("arg.compute", 0);

    
}

bool EmberOneCCLDisseminationBarrierGenerator::generate( std::queue<EmberEvent*>& evQ )
{

    int src,dst,mask;

    //std::cout << "Rank: " << rank() << " m_iterations = " << m_iterations << " m_loopIndex = " << m_loopIndex << std::endl;

    if ( m_loopIndex == m_iterations ) {

        //std::cout << "Rank: " << rank() << " m_iterations = " << m_iterations << " m_loopIndex = " << m_loopIndex << " final stage!" << std::endl;


        if ( 0 == rank() ) {
            double latency = (double)(m_stopTime-m_startTime)/(double)m_iterations;
            latency /= 1000000000.0;
            output( "%s: ranks %d, loop %d, latency %.3f us\n",
                    getMotifName().c_str(), size(), m_iterations, latency * 1000000.0  );

            //std::cout << "OneCCLDisseminationBarrier -> ending it" << std::endl;
        }

        //std::cout << "Rank: " << rank() << " Returning true" << std::endl;
        return true;
    }
    if ( 0 == m_loopIndex ) {
        enQ_getTime( evQ, &m_startTime );
    }

    enQ_compute( evQ, m_compute );
    
    //disseminationbarrier(evQ);


    //std::cout << "Inside Dissemination barrier method!" << std::endl;


    if (size() == 1) {
        return true;
    }

    mask = 0x1;

    while (mask < size()) {

        dst = (rank() + mask) % size() ;
        src = (rank() - mask + size() ) % size() ;

        enQ_send(evQ,m_addr,0,DOUBLE,dst,0,GroupWorld);
        //enQ_send(evQ,dst,1,0,GroupWorld);

        //std::cout << "Rank: " << rank() << " sending to Rank: " << dst << " mask: " << mask <<  std::endl;
        enQ_recv(evQ,m_addr,0,DOUBLE,src,0,GroupWorld);
        //enQ_recv(evQ,src,1,0,GroupWorld);

        //std::cout << "Rank: " << rank() << " receving from Rank: " << src << " mask: " << mask << std::endl;

        mask <<= 1;

    }

    if ( ++m_loopIndex == m_iterations ) {

        //std::cout << "Rank: " << rank() << " Exiting Dissemination Barrier method!" << std::endl;
        enQ_getTime( evQ, &m_stopTime );
    }
    return false;
}

void EmberOneCCLDisseminationBarrierGenerator::disseminationbarrier( std::queue<EmberEvent*>& evQ ) {


    int src,dst,mask;

    //std::cout << "Inside Dissemination barrier method!" << std::endl;


    if (size() == 1) {
        return;
    }

    mask = 0x1;

    while (mask < size()) {

        dst = (rank() + size()) % size() ;
        src = (rank() - mask + size() ) % size() ;

        //enQ_send(evQ,m_addr,0,DOUBLE,dst,0,GroupWorld);
        enQ_send(evQ,dst,8,0,GroupWorld);
        //enQ_recv(evQ,m_addr,0,DOUBLE,src,0,GroupWorld);
        enQ_recv(evQ,src,8,0,GroupWorld);

        mask <<= 1;

    }

    
}
