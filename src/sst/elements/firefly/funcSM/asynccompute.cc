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


#include <sst_config.h>

#include "funcSM/asynccompute.h"

using namespace SST::Firefly;

AsyncComputeFuncSM::AsyncComputeFuncSM( SST::Params& params ) :
    FunctionSMInterface(params),
    m_event( NULL )
{
    m_asynccompute_enterLatency  = (int) params.find("asynccompute_enterLatency", 1);
    m_asynccompute_returnLatency = (int) params.find("asynccompute_returnLatency", 1); 
}

void AsyncComputeFuncSM::handleStartEvent( SST::Event *e, Retval& retval )
{
    assert( NULL == m_event );
    m_event = static_cast< AsyncComputeStartEvent* >(e);

    m_dbg.debug(CALL_INFO,1,0,"AsyncCompute computetime=%d \n", m_event->computetime);

    proto()->asyncCompute(m_event->computetime,m_event->req);	

}

void AsyncComputeFuncSM::handleEnterEvent( Retval& retval )
{
    delete m_event;
    m_event = NULL;
    retval.setExit(0);
}
