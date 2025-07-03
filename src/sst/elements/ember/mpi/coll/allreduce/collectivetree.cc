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

//Default Allreduce Algorithm used in SST: CollectiveTree (https://github.com/sstsimulator/sst-elements/blob/master/src/sst/elements/firefly/funcSM/collectiveTree.cc)
//We are implementing it in Ember to compare it with other CCL allreduce implementations

#include <sst_config.h>
#include "../motifs/emberonecclallreduce.h"

//#include "collectivetree.h"

using namespace SST::Ember;
using namespace SST::Hermes::MP;

EmberOneCCLAllreduceGenerator::CollectiveTree::CollectiveTree(EmberOneCCLAllreduceGenerator& parent, Params& params, int iter) : allreduce(parent) , iterations(iter) , loopIndex(0) {

    
    //m_world = allreduce.size();
    m_world = allreduce.size();
    m_count = (uint32_t) params.find("arg.count", 1);
    m_rank = allreduce.rank();





}


void EmberOneCCLAllreduceGenerator::CollectiveTree::configure(){




    //std::cout << "Inside CollectiveTree::configure method!" << std::endl;

    m_yyy = new YYY(2,m_rank,m_world,0);


    
    
    

    m_recvReqV.resize(m_yyy->numChildren());
    m_sendReqV.resize(m_yyy->numChildren());
    m_bufV.resize(m_yyy->numChildren()+1);

    m_bufLen = m_count * allreduce.sizeofDataType(DOUBLE);

    allreduce.memSetBacked();

    for (int i =0; i < m_yyy->numChildren()+1; i++){

        m_bufV[i] = allreduce.memAlloc(m_bufLen);
    }

    m_waitUpState.init();
    m_sendDownState.init();
    m_state = WaitUp;

    //std::cout << "Exiting CollectiveTree::configure method!" << std::endl;

}



void EmberOneCCLAllreduceGenerator::CollectiveTree::generate(std::queue<EmberEvent*>& evQ){

    //std::cout << "inside collectivetree->generate method!" << std::endl;

    int child;


        switch (m_state) {

            case WaitUp:

                if (m_yyy->numChildren()){

                    switch (m_waitUpState.state) {

                        case WaitUpState::Posting:




			    for (int i=0; i < m_yyy->numChildren() ; i++) {

				    child = i;
			    
			    

                            	    allreduce.enQ_irecv(evQ,m_bufV[child+1],m_count,DOUBLE,m_yyy->calcChild(child),0,GroupWorld,&m_recvReqV[child]);
			    
			    
			    
			    
			    
			    }

			    child = 0;
			    m_waitUpState.state = WaitUpState::Waiting;


			
			    /*	
                            child = m_waitUpState.count;

                            ++m_waitUpState.count;

                            if (m_waitUpState.count == m_yyy->numChildren()){
                                m_waitUpState.count = 0;
                                m_waitUpState.state = WaitUpState::Waiting;
                            }


                            allreduce.enQ_irecv(evQ,m_bufV[child+1],m_count,DOUBLE,m_yyy->calcChild(child),0,GroupWorld,&m_recvReqV[child]);
                            return;
			    */

                        case WaitUpState::Waiting:

                            allreduce.enQ_waitall(evQ,m_yyy->numChildren(),&m_recvReqV[0],NULL);
			    

			    

			    /*
                            
			    if (m_yyy->parent() != -1) {
                                m_state = SendUp;
                            }
                            else {
                                m_state = SendDown;

                            }
                            break;
			    */


                    }


                }
		m_state = SendUp;


            case SendUp:

                if (-1 != m_yyy->parent()){

                    m_state = WaitDown;


                    allreduce.enQ_send(evQ,m_bufV[0],m_count,DOUBLE,m_yyy->parent(),0,GroupWorld);


                    //return;

                }

            
            case WaitDown:

                if (m_yyy->parent() != -1) {

                    m_state = SendDown;


                    allreduce.enQ_recv(evQ,m_bufV[0],m_count,DOUBLE,m_yyy->parent(),0,GroupWorld);
		    //return;


                }

            case SendDown:

                if (m_yyy->numChildren()) {

                    switch (m_sendDownState.state) {

                        case SendDownState::Sending:


			    for (int i=0; i < m_yyy->numChildren() ; i++) {


				    child = i;


                            	    allreduce.enQ_isend(evQ,m_bufV[0],m_count,DOUBLE,m_yyy->calcChild(child),0,GroupWorld,&m_sendReqV[child]);


			    }
			    child = 0;

			    m_sendDownState.state = SendDownState::Waiting;



                            /* 
                            child = m_sendDownState.count;
                            ++m_sendDownState.count;

                            if (m_sendDownState.count == m_yyy->numChildren()) {
                                m_sendDownState.count = 0;
                                m_sendDownState.state = SendDownState::Waiting;
                           
			    }


                            allreduce.enQ_isend(evQ,m_bufV[0],m_count,DOUBLE,m_yyy->calcChild(child),0,GroupWorld,&m_sendReqV[child]);
				

                            return;
			    */

                        case SendDownState::Waiting:


			    m_state = Exit;
                            allreduce.enQ_waitall(evQ,m_yyy->numChildren(),&m_sendReqV[0],NULL);



                            //return;



                    }
                }

	
	   case Exit:

		delete m_yyy;


            


        }








}




EmberOneCCLAllreduceGenerator::CollectiveTree::~CollectiveTree(){

    for (int i =0; i < m_yyy->numChildren()+1; i++){

        allreduce.memFree(m_bufV[i]);
    }
}
