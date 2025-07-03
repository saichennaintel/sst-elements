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

#include <sst_config.h>
#include "embercollallreduce.h"


using namespace SST::Ember;
using namespace SST::Hermes::MP;

static void test(void* a, void* b, int* len, PayloadDataType* ) {

	printf("%s() len=%d\n",__func__,*len);
}

EmberCollAllreduceGenerator::EmberCollAllreduceGenerator(SST::ComponentId_t id, Params& params): EmberMessagePassingGenerator(id, params, "CollAllreduce"),m_loopIndex(0)
{

    m_iterations = (uint32_t) params.find("arg.iterations",1);
    m_compute = (uint32_t) params.find("arg.compute",0);
    m_count = (uint32_t) params.find("arg.count",1);

    m_algorithm = params.find<std::string>("arg.algo","rabenseifner");


    if (m_algorithm == "rabenseifner") {

        m_algo = new Rabenseifner(*this,params, m_iterations);

    }

    else if (m_algorithm == "ring") {

        bool inplace = params.find<bool>("arg.inplace",false);

        m_algo = new Ring(*this,params,m_iterations, inplace);

    }

    else if (m_algorithm == "collectivetree") {

        m_algo = new CollectiveTree(*this,params,m_iterations);

    }

    else if (m_algorithm == "taha"){

        m_algo = new TAHA(*this,params,m_iterations);

    }


    else {
    
    	std::cout << "Invalid Coll Allreduce Algorithm selected: " << m_algorithm << std::endl;
    }



    configure(params);


}

void EmberCollAllreduceGenerator::configure(Params& params){



    if (m_algorithm == "rabenseifner") {

        Rabenseifner* rab = static_cast<Rabenseifner*>(m_algo);
        rab->configure();

    }

    else if (m_algorithm == "ring") {

        Ring *ring = static_cast<Ring*>(m_algo);
        ring->configure();

    }

    else if (m_algorithm == "collectivetree") {

        CollectiveTree *ctree = static_cast<CollectiveTree*>(m_algo);
        ctree->configure();

    }

    else if (m_algorithm == "taha"){

        TAHA *taha = static_cast<TAHA*>(m_algo);
        taha->configure();

    }


    else {
    
    	std::cout << "Invalid Coll Allreduce Algorithm selected: " << m_algorithm << std::endl;
    }
  
    

}

void EmberCollAllreduceGenerator::generate_algo(std::queue<EmberEvent*>& evQ) {

    
    if (m_algorithm == "rabenseifner") {

        Rabenseifner* rab = static_cast<Rabenseifner*>(m_algo);
        rab->generate(evQ);


    }

    
    else if (m_algorithm == "ring") {

        Ring *ring = static_cast<Ring*>(m_algo);
        ring->generate(evQ);
    }

    else if (m_algorithm == "collectivetree") {

        CollectiveTree *ctree = static_cast<CollectiveTree*>(m_algo);
        ctree->generate(evQ);
    }

    else if (m_algorithm == "taha"){

        TAHA *taha = static_cast<TAHA*>(m_algo);
        taha->generate(evQ);
    }

    

    else {
    
    	std::cout << "Invalid Coll Allreduce Algorithm selected: " << m_algorithm << std::endl;
    }

}

bool EmberCollAllreduceGenerator::generate(std::queue<EmberEvent*>& evQ){

    
    if ( m_loopIndex == m_iterations ) {


        //std::cout << "Rank: " << rank() << " Exiting the motif generator's generate method! " << " m_iterations = " << m_iterations << " m_loopIndex = " << m_loopIndex << std::endl;


        if ( 0 == rank() ) {
            double latency = (double)(m_stopTime-m_startTime)/(double)m_iterations;
            latency /= 1000000000.0;
            output( "%s: ranks %d, loop %d, %d double(s), %d message size, algorithm: %s, latency %.3f us\n",
                    getMotifName().c_str(), size(), m_iterations, m_count, m_count * sizeofDataType(DOUBLE), m_algorithm.c_str(), latency * 1000000.0  );
        }
        return true;
    }
    if ( 0 == m_loopIndex ) {
        enQ_getTime( evQ, &m_startTime );
    }

    enQ_compute( evQ, m_compute );

    //std::cout << "Rank: " << rank() << " Calling m_algo->generate() method " << " m_iterations = " << m_iterations << " m_loopIndex = " << m_loopIndex << std::endl; 

    generate_algo(evQ);
    //m_algo->generate(evQ);

    if ( ++m_loopIndex == m_iterations ) {
        enQ_getTime( evQ, &m_stopTime );

         //std::cout << "Rank: " << rank() << " Calling the stop timer! " << " m_iterations = " << m_iterations << " m_loopIndex = " << m_loopIndex << std::endl;
    }
    return false;
}



