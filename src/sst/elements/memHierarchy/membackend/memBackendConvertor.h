// Copyright 2009-2025 NTESS. Under the terms
// of Contract DE-NA0003525 with NTESS, the U.S.
// Government retains certain rights in this software.
//
// Copyright (c) 2009-2025, NTESS
// All rights reserved.
//
// Portions are copyright of other developers:
// See the file CONTRIBUTORS.TXT in the top level directory
// of the distribution for more information.
//
// This file is part of the SST software package. For license
// information, see the LICENSE file in the top level directory of the
// distribution.


#ifndef __SST_MEMH_MEMBACKENDCONVERTOR__
#define __SST_MEMH_MEMBACKENDCONVERTOR__

#include <sst/core/subcomponent.h>
#include <sst/core/event.h>
#include <sst/core/warnmacros.h>

#include "sst/elements/memHierarchy/memEvent.h"
#include "sst/elements/memHierarchy/customcmd/customCmdMemory.h"

namespace SST {
namespace MemHierarchy {

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#endif

class MemBackend;

class MemBackendConvertor : public SubComponent {
  public:

/* ELI definitions for subclasses */
#define MEMBACKENDCONVERTOR_ELI_PARAMS {"debug_level",     "(uint) Debugging level: 0 (no output) to 10 (all output). Output also requires that SST Core be compiled with '--enable-debug'", "0"},\
            {"debug_mask",      "(uint) Mask on debug_level", "0"},\
            {"debug_location",  "(uint) 0: No debugging, 1: STDOUT, 2: STDERR, 3: FILE", "0"}

#define MEMBACKENDCONVERTOR_ELI_STATS { "cycles_with_issue",                  "Total cycles with successful issue to back end",   "cycles",   1 },\
            { "cycles_attempted_issue_but_rejected","Total cycles where an attempt to issue to backend was rejected (indicates backend full)", "cycles", 1 },\
            { "total_cycles",                       "Total cycles called at the memory controller",     "cycles",   1 },\
            { "requests_received_GetS",             "Number of GetS (read) requests received",          "requests", 1 },\
            { "requests_received_GetSX",            "Number of GetSX (read) requests received",         "requests", 1 },\
            { "requests_received_GetX",             "Number of GetX (read) requests received",          "requests", 1 },\
            { "requests_received_PutM",             "Number of PutM (write) requests received",         "requests", 1 },\
            { "requests_received_Write",            "Number of Write (write) requests received",         "requests", 1 },\
            { "outstanding_requests",               "Total number of outstanding requests each cycle",  "requests", 1 },\
            { "latency_GetS",                       "Total latency of handled GetS requests",           "cycles",   1 },\
            { "latency_GetSX",                      "Total latency of handled GetSX requests",          "cycles",   1 },\
            { "latency_GetX",                       "Total latency of handled GetX requests",           "cycles",   1 },\
            { "latency_Write",                      "Total latency of handled Write requests",           "cycles",   1 },\
            { "latency_PutM",                       "Total latency of handled PutM requests",           "cycles",   1 }

    SST_ELI_REGISTER_SUBCOMPONENT_API(SST::MemHierarchy::MemBackendConvertor, MemBackend*, uint32_t)

    typedef uint64_t ReqId;

    class BaseReq : public SST::Core::Serialization::serializable {
    public:

        enum class ReqType { BASE, MEM, CUSTOM };

        BaseReq( uint32_t reqId, ReqType(type) ) : m_reqId(reqId), m_type(type) { }
        virtual ~BaseReq() { }

        static uint32_t getBaseId( ReqId id) { return id >> 32; }
        virtual uint64_t id()   { return ((uint64_t)m_reqId << 32); }
        virtual void decrement() { }
        virtual void increment( uint32_t UNUSED(bytes) ) { }
        virtual bool isDone() { return true; } /* If we're asking, the answer is yes */
        virtual bool issueDone() { return true; } /* If we're asking, the answer is yes */
        virtual std::string getString() {
            std::ostringstream str;
            str << "ID: " << m_reqId << (isMemEv() ? " MemReq " : " CustomReq ");
            return str.str();
        }
        bool isMemEv() { return m_type == ReqType::MEM; }
        bool isCustCmd() { return m_type == ReqType::CUSTOM; }
        virtual const std::string getRqstr() { return ""; }

        BaseReq() { }
        virtual void serialize_order(SST::Core::Serialization::serializer& ser) override {
            SST_SER(m_reqId);
            SST_SER(m_type);
        }
        ImplementSerializable(SST::MemHierarchy::MemBackendConvertor::BaseReq)

    protected:
        uint32_t m_reqId;
        ReqType m_type;
    };

    class CustomReq : public BaseReq {
    public:
        CustomReq(Interfaces::StandardMem::CustomData * info, Event::id_type evId, std::string rqstr, uint32_t reqId) : BaseReq(reqId, BaseReq::ReqType::CUSTOM),
            m_info(info), m_evId(evId), m_rqstr(rqstr) { }
        ~CustomReq() { }

        Interfaces::StandardMem::CustomData * getInfo() { return m_info; }
        const std::string getRqstr() override { return m_rqstr; }
        Event::id_type getEvId() { return m_evId; }
        std::string getString() override {
            std::ostringstream str;
            str << " EvID: " << m_evId.first << "," << m_evId.second;
            str << " Rqstr: " << m_rqstr;
            str << " Data: " << m_info->getString();
            return BaseReq::getString() + str.str();
        }
        CustomReq() { }
        virtual void serialize_order(SST::Core::Serialization::serializer& ser) override {
            BaseReq::serialize_order(ser);
            SST_SER(m_info);
            SST_SER(m_rqstr);
            SST_SER(m_evId);
        }
        ImplementSerializable(SST::MemHierarchy::MemBackendConvertor::CustomReq)
    private:
        Interfaces::StandardMem::CustomData * m_info;
        std::string m_rqstr;
        Event::id_type m_evId;

    };

    class MemReq : public BaseReq {
      public:
        MemReq( MemEvent* event, uint32_t reqId ) : BaseReq(reqId, BaseReq::ReqType::MEM),
            m_event(event), m_offset(0), m_numReq(0) { }
        ~MemReq() { }

        static uint32_t getBaseId( ReqId id) { return id >> 32; }
        Addr baseAddr() { return m_event->getBaseAddr(); }
        Addr addr()     { return m_event->getBaseAddr() + m_offset; }

        uint32_t processed()    { return m_offset; }
        uint64_t id()           { return ((uint64_t)m_reqId << 32) | m_offset; }
        MemEvent* getMemEvent() { return m_event; }
        bool isWrite()          { return (m_event->getCmd() == Command::PutM || m_event->getCmd() == Command::Write); }
        uint32_t size()         { return m_event->getSize(); }
        const std::string getRqstr() override { return m_event->getRqstr(); }

        void increment( uint32_t bytes ) {
            m_offset += bytes;
            ++m_numReq;
        }
        void decrement( ) { --m_numReq; }
        bool issueDone() {
            return m_offset >= m_event->getSize();
        }
        bool isDone( ) {
            return ( m_offset >= m_event->getSize() && 0 == m_numReq );
        }

        std::string getString() {
            std::ostringstream str;
            str << "addr: " << addr() << " baseAddr: " << baseAddr() << " processed: " << processed();
            str << " isWrite: " << isWrite();
            return BaseReq::getString() + str.str();
        }

        MemReq() { }
        virtual void serialize_order(SST::Core::Serialization::serializer& ser) override {
            BaseReq::serialize_order(ser);
            SST_SER(m_event);
            SST_SER(m_offset);
            SST_SER(m_numReq);
        }
        ImplementSerializable(SST::MemHierarchy::MemBackendConvertor::MemReq)
      private:
        MemEvent*   m_event;
        uint32_t    m_offset;
        uint32_t    m_numReq;
    };

  public:

    MemBackendConvertor(ComponentId_t id, Params& params, MemBackend* backend, uint32_t request_width);
    void finish(Cycle_t endCycle);
    virtual size_t getMemSize();
    virtual bool clock( Cycle_t cycle );
    virtual void turnClockOff();
    virtual void turnClockOn(Cycle_t cycle);
    virtual void handleMemEvent(  MemEvent* );
    virtual void handleCustomEvent(Interfaces::StandardMem::CustomData*, Event::id_type, std::string);
    virtual uint32_t getRequestWidth();
    virtual bool isBackendClocked() { return m_clockBackend; }

    virtual const std::string getRequestor( ReqId reqId ) {
        uint32_t id = BaseReq::getBaseId(reqId);
        if ( m_pendingRequests.find( id ) == m_pendingRequests.end() ) {
            m_dbg.fatal(CALL_INFO, -1, "memory request not found\n");
        }

        return m_pendingRequests[id]->getRqstr();
    }

    virtual void setCallbackHandlers(std::function<void(Event::id_type,uint32_t)> responseCB, std::function<Cycle_t()> clockenableCB);

    // generates a MemReq for the target custom command
    // this is utilized by inherited ExtMemBackendConvertor's
    // such that all the requests are consolidated in one place

    // Serialization support
    MemBackendConvertor() { }
    virtual void serialize_order(SST::Core::Serialization::serializer& ser) override;
    ImplementVirtualSerializable(SST::MemHierarchy::MemBackendConvertor);

protected:
    virtual ~MemBackendConvertor() {
        while ( m_requestQueue.size()) {
            delete m_requestQueue.front();
            m_requestQueue.pop_front();
        }
    }

    void doResponse( ReqId reqId, uint32_t flags = 0 );
    inline void sendResponse( SST::Event::id_type id, uint32_t flags );

    MemBackend* m_backend;
    uint32_t    m_backendRequestWidth;

    bool m_clockBackend;

  private:
    virtual bool issue(BaseReq*) = 0;




    bool setupMemReq( MemEvent* ev ) {
        if ( Command::FlushLine == ev->getCmd() || Command::FlushLineInv == ev->getCmd() ) {
            // TODO optimize if this becomes a problem, it is slow
            std::set<SST::Event::id_type> dependsOn;
            for (std::deque<BaseReq*>::iterator it = m_requestQueue.begin(); it != m_requestQueue.end(); it++) {
                if (!(*it)->isMemEv())
                    continue;
                MemReq * mr = static_cast<MemReq*>(*it);
                if (mr->baseAddr() == ev->getBaseAddr()) {
                    MemEvent * req = mr->getMemEvent();
                    dependsOn.insert(req->getID());
                    if (m_dependentRequests.find(req->getID()) == m_dependentRequests.end()) {
                        std::set<MemEvent*, memEventCmp> flushSet;
                        flushSet.insert(ev);
                        m_dependentRequests.insert(std::make_pair(req->getID(), flushSet));
                    } else {
                        (m_dependentRequests.find(req->getID())->second).insert(ev);
                    }
                }
            }

            if (dependsOn.empty()) return false;
            m_waitingFlushes.insert(std::make_pair(ev, dependsOn));
            return true;
        }

        uint32_t id = genReqId();
        MemReq* req = new MemReq( ev, id );
        m_requestQueue.push_back( req );
        m_pendingRequests[id] = req;
        return true;
    }

    inline void doClockStat( ) {
        stat_totalCycles->addData(1);
    }

    void doReceiveStat( Command cmd) {
        switch (cmd ) {
            case Command::GetS:
                stat_GetSReqReceived->addData(1);
                break;
            case Command::GetX:
                stat_GetXReqReceived->addData(1);
                break;
            case Command::GetSX:
                stat_GetSXReqReceived->addData(1);
                break;
            case Command::Write:
                stat_WriteReqReceived->addData(1);
                break;
            case Command::PutM:
                stat_PutMReqReceived->addData(1);
                break;
            default:
                break;
        }
    }

    void doResponseStat( Command cmd, Cycle_t latency ) {
        switch (cmd) {
            case Command::GetS:
                stat_GetSLatency->addData(latency);
                break;
            case Command::GetSX:
                stat_GetSXLatency->addData(latency);
                break;
            case Command::GetX:
                stat_GetXLatency->addData(latency);
                break;
            case Command::Write:
                stat_WriteLatency->addData(latency);
                break;
            case Command::PutM:
                stat_PutMLatency->addData(latency);
                break;
            default:
                break;
        }
    }

    Output      m_dbg;

    uint64_t m_cycleCount;

    bool m_clockOn;

    // Callback functions to parent component
    std::function<Cycle_t()> m_enableClock; // Re-enable parent's clock
    std::function<void(Event::id_type id, uint32_t)> m_notifyResponse; // notify parent of response

    uint32_t genReqId( ) { return ++m_reqId; }

    uint32_t m_reqId;

    typedef std::map<uint32_t,BaseReq*> PendingRequests;

    std::deque<BaseReq*>    m_requestQueue;
    PendingRequests         m_pendingRequests;
    uint32_t                m_frontendRequestWidth;

    std::map<MemEvent*, std::set<SST::Event::id_type> > m_waitingFlushes; // Set of request IDs for each flush
    std::map<SST::Event::id_type, std::set<MemEvent*, memEventCmp> > m_dependentRequests; // Reverse map, set of flushes for each request ID, for faster lookup

    Statistic<uint64_t>* stat_GetSLatency;
    Statistic<uint64_t>* stat_GetSXLatency;
    Statistic<uint64_t>* stat_GetXLatency;
    Statistic<uint64_t>* stat_WriteLatency;
    Statistic<uint64_t>* stat_PutMLatency;

    Statistic<uint64_t>* stat_GetSReqReceived;
    Statistic<uint64_t>* stat_GetXReqReceived;
    Statistic<uint64_t>* stat_WriteReqReceived;
    Statistic<uint64_t>* stat_PutMReqReceived;
    Statistic<uint64_t>* stat_GetSXReqReceived;

    Statistic<uint64_t>* stat_cyclesWithIssue;
    Statistic<uint64_t>* stat_cyclesAttemptIssueButRejected;
    Statistic<uint64_t>* stat_totalCycles;
    Statistic<uint64_t>* stat_outstandingReqs;

};

#if defined(__clang__)
#pragma clang diagnostic pop
#endif
}
}
#endif
