#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>
#include <stdio.h>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4
#define my_log(format, ...)                                                                                      \
	do                                                                                                           \
	{                                                                                                            \           
		FILE *file;                                                                                              \        
		file = fopen("../mr_log.log", "a");                                                                            \
		time_t time_val = time(NULL);                                                                            \
		char *ctr = ctime(&time_val);                                                                            \
		ctr[24] = '\0';                                                                                          \
		fprintf(file, #format " \n", ##__VA_ARGS__); \
		fclose(file);                                                                                         \
	} while (0)

enum mr_tasktype
{
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol
{
public:
	typedef int status;
	enum xxstatus
	{
		OK,
		RPCERR,
		NOENT,
		IOERR
	};
	enum rpc_numbers
	{
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse
	{
		// Lab4: Your definition here.
		int taskType;
		unsigned index;
		string fileName;
	};

	friend marshall &operator<<(marshall &m, const AskTaskResponse &res)
	{
		return m << res.taskType << res.index << res.fileName;
	}

	friend unmarshall &operator>>(unmarshall &u, AskTaskResponse &res)
	{
		return u >> res.taskType >> res.index >> res.fileName;
	}

	struct AskTaskRequest
	{
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse
	{
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest
	{
		// Lab4: Your definition here.
	};
};

#endif
