/****************/
/* Oleksandr Diachenko	*/
/* Date		*/
/* CS 244B	*/
/* Spring 2016	*/
/****************/

#include <network.h>

enum {
	NormalReturn = 0, ErrorReturn = -1,
};

/* ------------------------------------------------------------------ */

#ifdef ASSERT_DEBUG
#define ASSERT(ASSERTION) \
 { assert(ASSERTION); }
#else
#define ASSERT(ASSERTION) \
{ }
#endif

/* ------------------------------------------------------------------ */

/********************/
/* Server Functions */
/********************/
#ifdef __cplusplus
extern "C" {
#endif

extern int main(int argc, char* argv[]);
extern void processPacket(uint8_t *packet, int len);
extern void handleEvent(BaseEvent *event);
extern uint8_t beginTransaction(string clientId, string fileName);

#ifdef __cplusplus
}

#endif

/* ------------------------------------------------------------------ */

