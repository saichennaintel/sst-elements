// Copyright 2013-2023 NTESS. Under the terms
// of Contract DE-NA0003525 with NTESS, the U.S.
// Government retains certain rights in this software.
//
// Copyright (c) 2013-2023, NTESS
// All rights reserved.
//
// Portions are copyright of other developers:
// See the file CONTRIBUTORS.TXT in the top level directory
// of the distribution for more information.
//
// This file is part of the SST software package. For license
// information, see the LICENSE file in the top level directory of the
// distribution.

#ifndef COMPONENTS_FIREFLY_FUNCSM_ASYNCCOMPUTE_H
#define COMPONENTS_FIREFLY_FUNCSM_ASYNCCOMPUTE_H

#include "funcSM/api.h"
#include "funcSM/event.h"
#include "ctrlMsg.h"

namespace SST {
namespace Firefly {

class AsyncComputeFuncSM :  public FunctionSMInterface
{
  public:
    SST_ELI_REGISTER_MODULE(
        AsyncComputeFuncSM,
        "firefly",
        "AsyncCompute",
        SST_ELI_ELEMENT_VERSION(1,0,0),
        "",
        SST::Firefly::FunctionSMInterface
    )

  public:
    AsyncComputeFuncSM( SST::Params& params );

    virtual void handleStartEvent( SST::Event*, Retval& );
    virtual void handleEnterEvent( Retval& );

    virtual std::string protocolName() { return "CtrlMsgProtocol"; }

    //Included by Sai Chenna to facilitate enter and exit latencies for asynccompute.
    int asynccompute_enterLatency() { 
      //std::cout << "Entered asynccompute_enterLatency method!" << std::endl;
      return m_asynccompute_enterLatency; 
    
    }
    int asynccompute_returnLatency() { 
      //std::cout << "Entered asynccompute_returnLatency method!" << std::endl;
      return m_asynccompute_returnLatency; 
      
    }
    //===============================================================================

  private:

    CtrlMsg::API* proto() { return static_cast<CtrlMsg::API*>(m_proto); }

    AsyncComputeStartEvent*         m_event;

    //Included by Sai Chenna to facilitate enter and exit latencies for asynccompute.
    int             m_asynccompute_enterLatency;
    int             m_asynccompute_returnLatency;
    //===============================================================================
};

}
}

#endif
