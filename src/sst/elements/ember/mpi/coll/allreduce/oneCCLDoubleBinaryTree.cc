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

//Allreduce Algorithm: OneCCL Double Binary Tree 
// SST implementation is derived in https://github.com/oneapi-src/oneCCL/blob/master/src/coll/algorithms/double_tree_ops.cpp

#include <sst_config.h>
#include "../motifs/emberonecclallreduce.h"

using namespace SST::Ember;
using namespace SST::Hermes::MP;



EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::OneCCLDoubleTree(EmberOneCCLAllreduceGenerator& parent, Params& params, int iterations) : allreduce(parent) , iterations(iterations) , loopIndex(0) {

    
    //m_world = allreduce.size();
    m_world = allreduce.size();
    m_count = (uint32_t) params.find("arg.count", 1);
    m_rank = allreduce.rank();
    inplace = params.find<bool>("arg.inplace",false);




}


void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::configure(){

    d_tree = new ccl_double_tree((int)m_world,(int)m_rank);

    //d_tree = new nccl_double_tree((int)m_world,(int)m_rank);


    

    t1_work_count = m_count / 2 ;
    t2_work_count = m_count - t1_work_count ;

    std::cout << "Rank: " << m_rank << " t1_work_count = " << t1_work_count << std::endl;
    std::cout << "Rank: " << m_rank << " t2_work_count = " << t2_work_count << std::endl;


    t1_children = d_tree->T1().getchildren();
    t2_children = d_tree->T2().getchildren();

    t1_recvReqV.resize(t1_children);
    t1_sendReqV.resize(t1_children);
    t1_bufV.resize(t1_children+1); 

    t2_recvReqV.resize(t2_children);
    t2_sendReqV.resize(t2_children);
    t2_bufV.resize(t2_children+1);

    t1_sendrecvReqV.resize(2);
    t2_sendrecvReqV.resize(2); 


    t1_bufLen = t1_work_count * allreduce.sizeofDataType(DOUBLE);
    t2_bufLen = t2_work_count * allreduce.sizeofDataType(DOUBLE);

    allreduce.memSetBacked();

    for (int i =0; i < t1_children+1; i++){

        t1_bufV[i] = allreduce.memAlloc(t1_bufLen);
    }

    for (int i =0; i < t2_children+1; i++){

        t2_bufV[i] = allreduce.memAlloc(t2_bufLen); 
    }

    t1_waitUpState.init();
    t2_waitUpState.init();
    t1_sendDownState.init();
    t2_sendDownState.init();
    t1_state = WaitUp;
    t2_state = WaitUp;

    graphinfo(1);
    graphinfo(2);




    

}


void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::graphinfo(int phase) {

    if (phase == 1) {

        //int t1_children = d_tree->T1().getchildren();

        std::cout << "Rank: " << m_rank << " has " << t1_children << " children in T1 tree" << std::endl;
        std::cout << "Rank: " << m_rank << " parent is " << d_tree->T1().parent() << " in T1 tree" << std::endl;

        if (t1_children) {

            std::cout << "Rank: " << m_rank << " 's left child is " << d_tree->T1().left() << " in T1 tree" << std::endl;
            std::cout << "Rank: " << m_rank << " 's right child is " << d_tree->T1().right() << " in T1 tree" << std::endl; 
            
        
        }

    }

    if (phase == 2) {

        //int t2_children = d_tree->T2().getchildren();

        std::cout << "Rank: " << m_rank << " has " << t2_children << " children in T2 tree" << std::endl;
        std::cout << "Rank: " << m_rank << " parent is " << d_tree->T2().parent() << " in T2 tree" << std::endl;

        if (t2_children) {

            std::cout << "Rank: " << m_rank << " 's left child is " << d_tree->T2().left() << " in T2 tree" << std::endl;
            std::cout << "Rank: " << m_rank << " 's right child is " << d_tree->T2().right() << " in T2 tree" << std::endl; 
            
        
        }
    }



}



void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::generate(std::queue<EmberEvent*>& evQ){

    t1_waitUpState.init();
    t2_waitUpState.init();
    t1_sendDownState.init();
    t2_sendDownState.init();
    t1_state = WaitUp;
    t2_state = WaitUp;




    if (m_rank % 2 == 0) {

        std::cout << "My Rank is " << m_rank << " and I'm taking the even path!" << std::endl;

        reduce_broadcastv2(evQ,2);

        allreduce.enQ_barrier( evQ, GroupWorld );

        reduce_broadcastv2(evQ,1);



    }

    else {

        std::cout << "My Rank is " << m_rank << " and I'm taking the odd path!" << std::endl;

        reduce_broadcastv2(evQ,1);

        allreduce.enQ_barrier( evQ, GroupWorld );

        reduce_broadcastv2(evQ,2);


    }

    allreduce.enQ_barrier( evQ, GroupWorld );




    

}


void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::reduce_broadcastv3(std::queue<EmberEvent*>& evQ, int phase) {


    if (phase == 1) {

        if (d_tree->T1().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_recv : Rank1: " << m_rank << " Rank2: " << d_tree->T1().left() << std:: endl; 


            allreduce.enQ_recv(evQ,t1_bufV[1],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld);
        }

        if (d_tree->T1().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_recv : Rank1: " << m_rank << " Rank2: " << d_tree->T1().right() << std:: endl;

            allreduce.enQ_recv(evQ,t1_bufV[1],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld);
        }

        if (d_tree->T1().parent() != -1) {


            if (d_tree->T1().left() != -1 || d_tree->T1().right() != -1 ) {

                allreduce.enQ_barrier( evQ, GroupWorld );
            }

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_send : Rank1: " << m_rank << " sending reduced data to parent Rank2:  " << d_tree->T1().parent() << std:: endl;

            allreduce.enQ_send(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);

            
            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_recv : Rank1: " << m_rank << " received final data from parent Rank2:  " << d_tree->T1().parent() << std:: endl;

            allreduce.enQ_recv(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);

            

        }

        if (d_tree->T1().left() != -1 || d_tree->T1().right() != -1) {

                allreduce.enQ_barrier( evQ, GroupWorld );
        }    

        if (d_tree->T1().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_send : Rank1: " << m_rank << " to left child Rank2: " << d_tree->T1().left() << std:: endl;

            //Send to left
            allreduce.enQ_send(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld);
        }

        if (d_tree->T1().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 1 calling allreduce.enQ_send : Rank1: " << m_rank << " to right child Rank2: " << d_tree->T1().right() << std:: endl;

            //Send to right
            allreduce.enQ_send(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld);

        }


    }

    else {

        if (d_tree->T2().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_recv : Rank1: " << m_rank << " Rank2: " << d_tree->T2().left() << std:: endl;

            allreduce.enQ_recv(evQ,t2_bufV[1],t2_work_count,DOUBLE,d_tree->T2().left(),0,GroupWorld);
        }

        if (d_tree->T2().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_recv : Rank1: " << m_rank << " Rank2: " << d_tree->T2().right() << std:: endl;

            allreduce.enQ_recv(evQ,t2_bufV[2],t2_work_count,DOUBLE,d_tree->T2().right(),0,GroupWorld);
        }

        if (d_tree->T2().parent() != -1) {


            if (d_tree->T2().left() != -1 || d_tree->T2().right() != -1) {

                allreduce.enQ_barrier( evQ, GroupWorld );
            }

            
            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " sending reduced data to parent Rank2:  " << d_tree->T2().parent() << std:: endl;

            allreduce.enQ_send(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),0,GroupWorld);

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " receving final data to parent Rank2:  " << d_tree->T2().parent() << std:: endl;

            allreduce.enQ_recv(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),0,GroupWorld);


        }

        if (d_tree->T2().left() != -1 || d_tree->T2().right() != -1) {

                allreduce.enQ_barrier( evQ, GroupWorld );
        }

        

        if (d_tree->T2().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " to left child Rank2: " << d_tree->T2().left() << std:: endl;

            //Send to left
            allreduce.enQ_send(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().left(),0,GroupWorld);
        }

        if (d_tree->T2().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv3: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " to right child Rank2: " << d_tree->T2().right() << std:: endl;

            //Send to right
            allreduce.enQ_send(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().right(),0,GroupWorld);

        }


    }


}




void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::reduce_broadcastv2(std::queue<EmberEvent*>& evQ, int phase) {


    if (phase == 1) {

        if (d_tree->T1().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_irecv : Rank1: " << m_rank << " Rank2: " << d_tree->T1().left() << std:: endl; 

            allreduce.enQ_irecv(evQ,t1_bufV[1],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld,&t1_recvReqV[0]);
        }

        if (d_tree->T1().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_irecv : Rank1: " << m_rank << " Rank2: " << d_tree->T1().right() << std:: endl;

            allreduce.enQ_irecv(evQ,t1_bufV[2],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld,&t1_recvReqV[1]);
        }

        if (d_tree->T1().parent() != -1) {

            if (t1_children) {

                if (t1_children == 1) {

                    if (d_tree->T1().left() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for left child! Rank2: " << d_tree->T1().left() << std:: endl;

                        allreduce.enQ_wait(evQ,&t1_recvReqV[0],NULL);
                        

                    }

                    if (d_tree->T1().right() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for right child! Rank2: " << d_tree->T1().right()  << std:: endl;

                        allreduce.enQ_wait(evQ,&t1_recvReqV[1],NULL);
                    }

                    else {

                        std::cout << "ERROR!!" << std::endl;
                    }
                }

                else if (t1_children == 2) {

                    std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T1().left() << " and right Rank3: " << d_tree->T1().right() << " child! " << std:: endl;

                    allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);

                    

                }



            }

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_send : Rank1: " << m_rank << " sending reduced data to parent Rank2:  " << d_tree->T1().parent() << std:: endl;

            allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld,&t1_sendrecvReqV[0]);

            
            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_recv : Rank1: " << m_rank << " received final data from parent Rank2:  " << d_tree->T1().parent() << std:: endl;

            allreduce.enQ_irecv(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld,&t1_sendrecvReqV[1]);

            //allreduce.enQ_waitall(evQ,2,&t1_sendrecvReqV[0],NULL);

            




        }

        if ( d_tree->T1().parent() == -1 ) {

            if (t1_children == 1) {

                    if (d_tree->T1().left() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for left child! Rank2: " << d_tree->T1().left() << std:: endl;

                        allreduce.enQ_wait(evQ,&t1_recvReqV[0],NULL);

                        

                    }

                    if (d_tree->T1().right() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for right child! Rank2: " << d_tree->T1().right() << std:: endl;

                        allreduce.enQ_wait(evQ,&t1_recvReqV[1],NULL);

                        
                    }
            }

            else if (t1_children == 2) {

                    std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T1().left() << " and right Rank3: " << d_tree->T1().right() << " child! " << std:: endl;

                    allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);

            }

            
        }

        if (d_tree->T1().left() != -1 || d_tree->T1().right() != -1) {

            allreduce.enQ_waitall(evQ,2,&t1_sendrecvReqV[0],NULL);
        }

        if (d_tree->T1().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_isend : Rank1: " << m_rank << " to left child Rank2: " << d_tree->T1().left() << std:: endl;

            //Send to left
            allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld,&t1_sendReqV[0]);
        }

        if (d_tree->T1().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_isend : Rank1: " << m_rank << " to right child Rank2: " << d_tree->T1().right() << std:: endl;

            //Send to right
            allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld,&t1_sendReqV[1]);

        }

        if (t1_children == 1) {

            if (d_tree->T1().left() != -1) {

                std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for left child Rank2: " << d_tree->T1().left() << std:: endl;

                allreduce.enQ_wait(evQ,&t1_sendReqV[0],NULL);

            }

            if (d_tree->T1().right() != -1) {

                std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for right child Rank2: " << d_tree->T1().right() << std:: endl;

               allreduce.enQ_wait(evQ,&t1_sendReqV[1],NULL);
            }
                
        }

        else if (t1_children == 2) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 1 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T1().left() << " and right Rank3: " << d_tree->T1().right() << " child! " << std:: endl;

            allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);

        }

            


    }

    else {

        if (d_tree->T2().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_irecv : Rank1: " << m_rank << " Rank2: " << d_tree->T2().left() << std:: endl;

            allreduce.enQ_irecv(evQ,t2_bufV[1],t2_work_count,DOUBLE,d_tree->T2().left(),5,GroupWorld,&t2_recvReqV[0]);
        }

        if (d_tree->T2().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_irecv : Rank1: " << m_rank << " Rank2: " << d_tree->T2().right() << std:: endl;

            allreduce.enQ_irecv(evQ,t2_bufV[2],t2_work_count,DOUBLE,d_tree->T2().right(),5,GroupWorld,&t2_recvReqV[1]);
        }

        if (d_tree->T2().parent() != -1) {

            if (t2_children) {

                if (t2_children == 1) {

                    if (d_tree->T2().left() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for left child! Rank2: " << d_tree->T2().left() << std:: endl;

                        allreduce.enQ_wait(evQ,&t2_recvReqV[0],NULL);

                    }

                    if (d_tree->T2().right() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for right child! Rank2: " << d_tree->T2().right() << std:: endl;

                        allreduce.enQ_wait(evQ,&t2_recvReqV[1],NULL);
                    }
                }

                else if (t2_children == 2) {

                    std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T2().left() << " and right Rank3: " << d_tree->T2().right() << " child! " << std:: endl;

                    allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);

                }



            }

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " sending reduced data to parent Rank2:  " << d_tree->T2().parent() << std:: endl;

            allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),5,GroupWorld,&t2_sendrecvReqV[0]);

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_send : Rank1: " << m_rank << " receving final data to parent Rank2:  " << d_tree->T2().parent() << std:: endl;

            allreduce.enQ_irecv(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),5,GroupWorld,&t2_sendrecvReqV[1]);


            //allreduce.enQ_waitall(evQ,2,&t2_sendrecvReqV[0],NULL);


        }

        if (d_tree->T2().parent() == -1 ) {

            if (t2_children == 1) {

                    if (d_tree->T2().left() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for left child Rank2: " << d_tree->T2().left() << std:: endl;

                        allreduce.enQ_wait(evQ,&t2_recvReqV[0],NULL);

                    }

                    if (d_tree->T2().right() != -1) {

                        std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for right child Rank2: " << d_tree->T2().right() << std:: endl;

                        allreduce.enQ_wait(evQ,&t2_recvReqV[1],NULL);
                    }
            }

            else if (t2_children == 2) {

                    std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T2().left() << " and right Rank3: " << d_tree->T2().right() << " child! " << std:: endl;

                    allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);

            }

            
        }

        if (d_tree->T2().left() != -1 || d_tree->T2().right() != -1) {

            allreduce.enQ_waitall(evQ,2,&t1_sendrecvReqV[0],NULL);
        }

        if (d_tree->T2().left() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_isend : Rank1: " << m_rank << " to left child Rank2: " << d_tree->T2().left() << std:: endl;

            //Send to left
            allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().left(),5,GroupWorld,&t2_sendReqV[0]);
        }

        if (d_tree->T2().right() != -1) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_isend : Rank1: " << m_rank << " to right child Rank2: " << d_tree->T2().right() << std:: endl;

            //Send to right
            allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().right(),5,GroupWorld,&t2_sendReqV[1]);

        }

        if (t2_children == 1) {

            if (d_tree->T2().left() != -1) {

                std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left child Rank2: " << d_tree->T2().left() << std:: endl;

                allreduce.enQ_wait(evQ,&t2_sendReqV[0],NULL);

            }

            if (d_tree->T2().right() != -1) {

                std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both right child Rank2: " << d_tree->T2().right() << std:: endl;

                allreduce.enQ_wait(evQ,&t2_sendReqV[1],NULL);
            }
                
        }

        else if (t2_children == 2) {

            std::cout << " Inside OneCCLDoubleTree::reduce_broadcastv2: Phase 2 calling allreduce.enQ_waitall : Rank1: " << m_rank << " Waiting for both left Rank2: " << d_tree->T2().left() << " and right Rank3: " << d_tree->T2().right() << " child! " << std:: endl;

            allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);

        }







    }


}

void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::reduce_broadcast(std::queue<EmberEvent*>& evQ, int phase) {

    //const auto& tree;

    //int child;

    if (phase == 1) {

        switch (t1_state) {


            case WaitUp:

                if (t1_children) {

                    switch (t1_waitUpState.state) {

                        case WaitUpState::Posting:


                            if (d_tree->T1().left() != -1) {

                                allreduce.enQ_irecv(evQ,t1_bufV[1],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld,&t1_recvReqV[0]);

                            }

                            if (d_tree->T1().right() != -1) {



                                allreduce.enQ_irecv(evQ,t1_bufV[2],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld,&t1_recvReqV[1]);
                            }

                            t1_waitUpState.state = WaitUpState::Waiting;

                        
                        case WaitUpState::Waiting:

                            if (t1_children == 1) {

                                if (d_tree->T1().left() != -1) {

                                    allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);

                                }

                                if (d_tree->T1().right() != -1) {

                                    allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[1],NULL);
                                }
                            }

                            if (t1_children > 1) {

                                allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);

                            }


                    }
                }

                t1_state = SendUp;

            
            case SendUp:


                if (d_tree->T1().parent() != -1) {

                    t1_state = WaitDown;

                    allreduce.enQ_send(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);


                }


            case WaitDown:

                if (d_tree->T1().parent() != -1) {

                    t1_state = SendDown;

                    allreduce.enQ_recv(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);


                }

            case SendDown:

                if (t1_children) {

                    switch(t1_sendDownState.state) {

                        case SendDownState::Sending:

                            if (d_tree->T1().left() != -1) {

                                allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().left(),0,GroupWorld,&t1_sendReqV[0]);
                            }

                            if (d_tree->T1().right() != -1) {

                                allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().right(),0,GroupWorld,&t1_sendReqV[1]);
                            }


                            t1_sendDownState.state = SendDownState::Waiting;



                        case SendDownState::Waiting:

                            //t1_state = Exit;

                            if (t1_children == 1) {

                                if (d_tree->T1().left() != -1) {

                                    allreduce.enQ_waitall(evQ,t1_children,&t1_sendReqV[0],NULL);
                                }

                                if (d_tree->T1().right() != -1) {

                                    allreduce.enQ_waitall(evQ,t1_children,&t1_sendReqV[1],NULL);
                                }
                            }

                            if (t1_children > 1) {

                                allreduce.enQ_waitall(evQ,t1_children,&t1_sendReqV[0],NULL);

                            }

                            


                    }


                }


                


        }




    }


    else {
        

        switch (t2_state) {


            case WaitUp:

                if (t2_children) {

                    switch (t2_waitUpState.state) {

                        case WaitUpState::Posting:


                            if (d_tree->T2().left() != -1) {

                                allreduce.enQ_irecv(evQ,t2_bufV[1],t2_work_count,DOUBLE,d_tree->T2().left(),1,GroupWorld,&t2_recvReqV[0]);

                            }

                            if (d_tree->T2().right() != -1) {



                                allreduce.enQ_irecv(evQ,t2_bufV[2],t2_work_count,DOUBLE,d_tree->T2().right(),1,GroupWorld,&t2_recvReqV[1]);
                            }

                            t2_waitUpState.state = WaitUpState::Waiting;

                        
                        case WaitUpState::Waiting:


                            if (t2_children == 1) {

                                if (d_tree->T2().left() != -1) {

                                    allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);
                                }

                                if (d_tree->T2().right() != -1) {
                                    allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[1],NULL);
                                }

                            }

                            if (t2_children > 1) {

                                allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);
                            }

                            


                    }
                }

                t2_state = SendUp;

            
            case SendUp:


                if (d_tree->T2().parent() != -1) {

                    t2_state = WaitDown;

                    allreduce.enQ_send(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),1,GroupWorld);


                }


            case WaitDown:

                if (d_tree->T2().parent() != -1) {

                    t2_state = SendDown;

                    allreduce.enQ_recv(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),1,GroupWorld);


                }

            case SendDown:

                if (t2_children) {

                    switch(t2_sendDownState.state) {

                        case SendDownState::Sending:

                            if (d_tree->T2().left() != -1) {

                                allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().left(),1,GroupWorld,&t2_sendReqV[0]);
                            }

                            if (d_tree->T2().right() != -1) {

                                allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().right(),1,GroupWorld,&t2_sendReqV[1]);
                            }


                            t2_sendDownState.state = SendDownState::Waiting;



                        case SendDownState::Waiting:

                            //t2_state = Exit;

                            if (t2_children == 1) {

                                if ( d_tree->T2().left() != -1) {

                                    allreduce.enQ_waitall(evQ,t2_children,&t2_sendReqV[0],NULL);
                                }

                                if ( d_tree->T2().right() != -1) {

                                    allreduce.enQ_waitall(evQ,t2_children,&t2_sendReqV[1],NULL);
                                }
                            }

                            if (t2_children > 1) {

                                allreduce.enQ_waitall(evQ,t2_children,&t2_sendReqV[0],NULL);

                            }

                            


                    }


                }



                


        }



    
    
    
    }

    

}



void EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::reduce_broadcastv4(std::queue<EmberEvent*>& evQ, int phase) {

    //const auto& tree;

    std::cout << "Inside reduce_broadcastv4 method!" << std::endl;

    int child;

    if (phase == 1) {

        switch (t1_state) {


            case WaitUp:

                if (t1_children) {

                    switch (t1_waitUpState.state) {

                        case WaitUpState::Posting:




                            for (int i=0; i < t1_children ; i++) {

                                child = i;

                                allreduce.enQ_irecv(evQ,t1_bufV[child+1],t1_work_count,DOUBLE,d_tree->T1().get_child(child),0,GroupWorld,&t1_recvReqV[child]);

                            }

                            child = 0;

                            t1_waitUpState.state = WaitUpState::Waiting;

                        
                        case WaitUpState::Waiting:


                            allreduce.enQ_waitall(evQ,t1_children,&t1_recvReqV[0],NULL);




                    }
                }

                t1_state = SendUp;

            
            case SendUp:


                if (d_tree->T1().parent() != -1) {

                    t1_state = WaitDown;

                    allreduce.enQ_send(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);


                }


            case WaitDown:

                if (d_tree->T1().parent() != -1) {

                    t1_state = SendDown;

                    allreduce.enQ_recv(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().parent(),0,GroupWorld);


                }

            case SendDown:

                if (t1_children) {

                    switch(t1_sendDownState.state) {

                        case SendDownState::Sending:


                            for (int i=0; i < t1_children ; i++) {

				                child = i;

                            	allreduce.enQ_isend(evQ,t1_bufV[0],t1_work_count,DOUBLE,d_tree->T1().get_child(child),0,GroupWorld,&t1_sendReqV[child]);


			                }
			                
                            child = 0;


                            t1_sendDownState.state = SendDownState::Waiting;



                        case SendDownState::Waiting:

                            //t1_state = Exit;



                            allreduce.enQ_waitall(evQ,t1_children,&t1_sendReqV[0],NULL);


                            


                    }


                }


                


        }




    }


    else {
        

        switch (t2_state) {


            case WaitUp:

                if (t2_children) {

                    switch (t2_waitUpState.state) {

                        case WaitUpState::Posting:




                            for (int i=0; i < t2_children ; i++) {


                                child = i;

                                allreduce.enQ_irecv(evQ,t2_bufV[child+1],t2_work_count,DOUBLE,d_tree->T2().get_child(child),1,GroupWorld,&t2_recvReqV[child]);


                            }


                            child = 0 ;
                            

                            t2_waitUpState.state = WaitUpState::Waiting;

                        
                        case WaitUpState::Waiting:


                            allreduce.enQ_waitall(evQ,t2_children,&t2_recvReqV[0],NULL);

                            


                    }
                }

                t2_state = SendUp;

            
            case SendUp:


                if (d_tree->T2().parent() != -1) {

                    t2_state = WaitDown;

                    allreduce.enQ_send(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),1,GroupWorld);


                }


            case WaitDown:

                if (d_tree->T2().parent() != -1) {

                    t2_state = SendDown;

                    allreduce.enQ_recv(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().parent(),1,GroupWorld);


                }

            case SendDown:

                if (t2_children) {

                    switch(t2_sendDownState.state) {

                        case SendDownState::Sending:

                            for (int i=0; i < t2_children ; i++) {


                                child = i;

                                allreduce.enQ_isend(evQ,t2_bufV[0],t2_work_count,DOUBLE,d_tree->T2().get_child(child),1,GroupWorld,&t2_sendReqV[child]);


                            }

                            child = 0;


                            t2_sendDownState.state = SendDownState::Waiting;



                        case SendDownState::Waiting:

                            //t2_state = Exit;

                            allreduce.enQ_waitall(evQ,t2_children,&t2_sendReqV[0],NULL);


                            


                    }


                }



                


        }



    
    
    
    }

    

}



EmberOneCCLAllreduceGenerator::OneCCLDoubleTree::~OneCCLDoubleTree(){


    for (int i =0; i < t1_children+1; i++){

        allreduce.memFree(t1_bufV[i]);
    }

    for (int i =0; i < t2_children+1; i++){

        allreduce.memFree(t2_bufV[i]); 
    }

}
