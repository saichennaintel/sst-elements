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


#include <sst_config.h>
#include "sst/elements/miranda/generators/streambench_customcmd.h"

#include <sst/core/params.h>

#include "sst/elements/miranda/generators/customcmd_opcode.h"

using namespace SST::Miranda;


STREAMBenchGenerator_CustomCmd::STREAMBenchGenerator_CustomCmd( ComponentId_t id, Params& params ) :
	RequestGenerator(id, params) {
            build(params);
        }

void STREAMBenchGenerator_CustomCmd::build( Params& params ) {

	const uint32_t verbose = params.find<uint32_t>("verbose", 0);

	out = new Output("STREAMBenchGenerator_CustomCmd[@p:@l]: ", verbose, 0, Output::STDOUT);

	n = params.find<uint64_t>("n", 10000);
	reqLength = params.find<uint64_t>("operandwidth", 8);

	start_a = params.find<uint64_t>("start_a", 0);
	const uint64_t def_b = start_a + (n * reqLength);

	start_b = params.find<uint64_t>("start_b", def_b);

	const uint64_t def_c = start_b + (n * reqLength);
	start_c = params.find<uint64_t>("start_c", def_c);

	n_per_call = params.find<uint64_t>("n_per_call", 1);

        custom_write_opcode = params.find<uint32_t>("write_cmd", 0xFFFF );

        custom_read_opcode = params.find<uint32_t>("read_cmd", 0xFFFF );

	i = 0;

	out->verbose(CALL_INFO, 1, 0, "STREAM-N length is %" PRIu64 "\n", n);
	out->verbose(CALL_INFO, 1, 0, "operandwidth       %" PRIu64 "\n", reqLength);
	out->verbose(CALL_INFO, 1, 0, "Start of array a @ 0x%" PRIx64 "\n", start_a);
	out->verbose(CALL_INFO, 1, 0, "Start of array b @ 0x%" PRIx64 "\n", start_b);
	out->verbose(CALL_INFO, 1, 0, "Start of array c @ 0x%" PRIx64 "\n", start_c);
	out->verbose(CALL_INFO, 1, 0, "Array Length:      %" PRIu64 " bytes\n", (n * reqLength));
	out->verbose(CALL_INFO, 1, 0, "Total arrays:      %" PRIu64 " bytes\n", (3 * n * reqLength));
	out->verbose(CALL_INFO, 1, 0, "N-per-generate     %" PRIu64 "\n", n_per_call);
        if( custom_write_opcode != 0xFFFF ){
          out->verbose(CALL_INFO, 1, 0, "Custom WR opcode   %" PRIu32 "\n", custom_write_opcode );
        }
        if( custom_read_opcode != 0xFFFF ){
          out->verbose(CALL_INFO, 1, 0, "Custom RD opcode   %" PRIu32 "\n", custom_read_opcode );
        }
}

STREAMBenchGenerator_CustomCmd::~STREAMBenchGenerator_CustomCmd() {
	delete out;
}

void STREAMBenchGenerator_CustomCmd::generate(MirandaRequestQueue<GeneratorRequest*>* q) {
	for(uint64_t j = 0; j < n_per_call; ++j) {
		out->verbose(CALL_INFO, 4, 0, "Array index: %" PRIu64 "\n", i);

		// If we reached our limit then step out of the generation
		if(i == n) {
			break;
		}

                GeneratorRequest* read_b;
                GeneratorRequest* read_c;
                GeneratorRequest* write_a;

                if( custom_read_opcode == 0xFFFF ){
                  // issue standard read
		  read_b  = new MemoryOpRequest(start_b + (i * reqLength),
                                                reqLength,
                                                READ);
		  read_c  = new MemoryOpRequest(start_c + (i * reqLength),
                                                reqLength,
                                                READ);
                }else{
                  // issue custom read
                  OpCodeStdMem* opc_b = new OpCodeStdMem(start_b + (i * reqLength),
                                                         reqLength,
                                                         custom_read_opcode);
                  OpCodeStdMem* opc_c = new OpCodeStdMem(start_c + (i * reqLength),
                                                         reqLength,
                                                         custom_read_opcode);
                  read_b = new CustomOpRequest(opc_b);
                  read_c = new CustomOpRequest(opc_c);
                }

                if( custom_write_opcode == 0xFFFF ){
                    // issue standard write
		    write_a = new MemoryOpRequest(start_a + (i * reqLength),
                                                  reqLength,
                                                  WRITE);
                }else{
                    // issue custom write
                    OpCodeStdMem* opc_a = new OpCodeStdMem(start_a + (i * reqLength),
                                                           reqLength,
                                                           custom_write_opcode);
		    write_a = new CustomOpRequest(opc_a);
                }

		write_a->addDependency(read_b->getRequestID());
		write_a->addDependency(read_c->getRequestID());

		out->verbose(CALL_INFO, 8, 0, "Issuing %s READ request for address %" PRIu64 "\n", (custom_read_opcode == 0xFFFF ? "regular" : "custom"), (start_b + (i * reqLength)));
		q->push_back(read_b);

		out->verbose(CALL_INFO, 8, 0, "Issuing %s READ request for address %" PRIu64 "\n", (custom_read_opcode == 0xFFFF ? "regular" : "custom"), (start_c + (i * reqLength)));
		q->push_back(read_c);

		out->verbose(CALL_INFO, 8, 0, "Issuing %s WRITE request for address %" PRIu64 "\n", (custom_write_opcode == 0xFFFF ? "regular" : "custom"), (start_a + (i * reqLength)));
		q->push_back(write_a);

		i++;
	}
}

bool STREAMBenchGenerator_CustomCmd::isFinished() {
	return (i == n);
}

void STREAMBenchGenerator_CustomCmd::completed() {

}
