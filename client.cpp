/****************/
/* Oleksandr Diachenko	*/
/* 05/22/2016		*/
/* CS 244B	*/
/* Spring 2016	*/
/****************/

#define DEBUG

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <sstream>
#include <list>
#include <map>

#include <client.h>
#include <network.h>

class Client {
public:

	Client(Network *network, int numServers) {
		this->numServers = numServers;
		this->network = network;

	}
	;
	string getClientId() {
		char hostname[1024];
		hostname[1023] = '\0';
		gethostname(hostname, 1023);
		std::ostringstream ss;
		ss << ::getppid()<<hostname;
		string clientId = ss.str();

		return clientId;
	}
	;

	int beginTransaction(string fileName) {

		BaseEvent * beginTransactionRequestEvent =
				new BeginTransactionRequestEvent(this->getClientId(),
						string(fileName));
		list<DfsPacket*> packets = this->network->sendPacketRetry(
				this->getClientId(), this->getNumServers(),
				beginTransactionRequestEvent,
				EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE);
		list<DfsPacket*>::const_iterator iterator;
		for (iterator = packets.begin(); iterator != packets.end();
				++iterator) {
			BeginTransactionResponseEvent *event =
					(BeginTransactionResponseEvent*) this->network->packetToEvent(
							*iterator);
			transactionsPerServersPerFd[fileDescriptorCounter][event->getSenderNodeId()] =
					event->getTransactionId();

			printf("Server %s opened transaction %d, file descriptor %d\n",
					event->getSenderNodeId().c_str(), event->getTransactionId(),
					fileDescriptorCounter);
		}

		return fileDescriptorCounter++;

	}
	;

	int writeBlock(int fd, char * buffer, int byteOffset, int blockSize) {

		map<string, uint8_t>::iterator it;

		for (it = transactionsPerServersPerFd[fd].begin();
				it != transactionsPerServersPerFd[fd].end(); it++) {
			WriteBlockEvent *event = new WriteBlockEvent(this->getClientId(),
					it->first, it->second, buffer, byteOffset, blockSize);
			this->network->sendPacket(event, true);
		}

		return blockSize;

	}

	int getNumServers() {
		return this->numServers;
	}

	int commit(int fd, bool closeFile) {

		int commitRequestStatus = this->prepareCommit(fd);

		if (commitRequestStatus == NormalReturn) {

			printf("Vote OK\n");
			int finishCommitStatus = this->finishCommit(fd, closeFile);

			return finishCommitStatus;

		} else {
			printf("Vote wasn't OK, rolling transaction back\n");
			this->rollback(fd);

			return ErrorReturn;
		}

	}

	int prepareCommit(int fd) {

		map<string, uint8_t>::iterator it;

		list<DfsPacket*> packets;
		int positiveVotesNumber = 0;

		for (it = transactionsPerServersPerFd[fd].begin();
				it != transactionsPerServersPerFd[fd].end(); it++) {

			CommitRequestEvent *commitRequestEvent = new CommitRequestEvent(
					this->getClientId(), it->first, it->second);

			list<DfsPacket*> packetsPerServer = this->network->sendPacketRetry(
					this->getClientId(), 1, commitRequestEvent,
					EVENT_TYPE_COMMIT_VOTE);

			packets.merge(packetsPerServer);

		}

		CommitVoteEvent *event;

		list<DfsPacket*>::const_iterator it_votes;
		for (it_votes = packets.begin(); it_votes != packets.end();
				++it_votes) {
			event = (CommitVoteEvent *) this->network->packetToEvent(*it_votes);

			if (event->getVote() == POSITIVE_VOTE)
				positiveVotesNumber++;

		}

		printf("Received commit votes, number of total votes: %d, positive votes number: %d\n",
				packets.size(), positiveVotesNumber);

		if (positiveVotesNumber < this->getNumServers()) {
			printf("Number of votes is not sufficient for commit\n");
			return ErrorReturn;
		}

		return NormalReturn;
	}

	int finishCommit(int fd, bool closeFile) {

		map<string, uint8_t>::iterator it;

		list<DfsPacket*> packets;

		for (it = transactionsPerServersPerFd[fd].begin();
				it != transactionsPerServersPerFd[fd].end(); it++) {

			CommitEvent *commitEvent = new CommitEvent(this->getClientId(),
					it->first, it->second, closeFile);

			list<DfsPacket*> packetsPerServer = this->network->sendPacketRetry(
					this->getClientId(), 1, commitEvent,
					EVENT_TYPE_COMMIT_ROLLBACK_ACK);

			packets.merge(packetsPerServer);

		}

		if (packets.size() == this->getNumServers())
			return NormalReturn;
		else
			return ErrorReturn;

	}

	int rollback(int fd) {

		map<string, uint8_t>::iterator it;

		list<DfsPacket*> packets;
		int positiveVotesNumber = 0;

		for (it = transactionsPerServersPerFd[fd].begin();
				it != transactionsPerServersPerFd[fd].end(); it++) {

			RollbackEvent *rollbackEvent = new RollbackEvent(
					this->getClientId(), it->first, it->second);

			list<DfsPacket*> packetsPerServer = this->network->sendPacketRetry(
					this->getClientId(), 1, rollbackEvent,
					EVENT_TYPE_COMMIT_ROLLBACK_ACK);

			packets.merge(packetsPerServer);

		}

		return NormalReturn;

	}

private:
	int fileDescriptorCounter;
	int numServers;
	Network *network;
	map<int, map<string, uint8_t> > transactionsPerServersPerFd;
};

static Network *n;
static Client *c;

/* ------------------------------------------------------------------ */

int InitReplFs(unsigned short portNum, int packetLoss, int numServers) {
#ifdef DEBUG
	printf("INITREPLFS: Port number %d, packet loss %d percent, %d servers\n",
			portNum, packetLoss, numServers);
#endif

	/****************************************************/
	/* Initialize network access, local state, etc.     */
	/****************************************************/

	n = new Network(portNum, packetLoss);
	n->netInit();

	c = new Client(n, numServers);

	return (NormalReturn);
}

/* ------------------------------------------------------------------ */

int OpenFile(char * fileName) {

	ASSERT(fileName);

#ifdef DEBUG
	printf("OPENFILE: Opening File '%s'\n", fileName);
#endif

	int fd = c->beginTransaction(fileName);

	if (fd < 0) {
		printf("Unable to open file\n");
		return ErrorReturn;

	}

	return fd;
}

/* ------------------------------------------------------------------ */

int WriteBlock(int fd, char * buffer, int byteOffset, int blockSize) {
	int bytesWritten;

	ASSERT( fd >= 0);
	ASSERT( byteOffset >= 0);
	ASSERT( buffer);
	ASSERT( blockSize >= 0 && blockSize < MaxBlockLength);

#ifdef DEBUG
	printf("WRITEBLOCK: Writing FD=%d, Offset=%d, Length=%d\n", fd, byteOffset,
			blockSize);
#endif

	bytesWritten = c->writeBlock(fd, buffer, byteOffset, blockSize);

	return (bytesWritten);

}

/* ------------------------------------------------------------------ */

int Commit(int fd) {
	ASSERT( fd >= 0);

#ifdef DEBUG
	printf("COMMIT: FD=%d\n", fd);
#endif

	/****************************************************/
	/* Prepare to Commit Phase			    */
	/* - Check that all writes made it to the server(s) */
	/****************************************************/

	/****************/
	/* Commit Phase */
	/****************/

	int commitStatus = c->commit(fd, false);

	return commitStatus;

}

/* ------------------------------------------------------------------ */

int Abort(int fd) {
	ASSERT( fd >= 0);

#ifdef DEBUG
	printf("ABORT: FD=%d\n", fd);
#endif

	/*************************/
	/* Abort the transaction */
	/*************************/

	int abortStatus = c->rollback(fd);

	return abortStatus;
}

/* ------------------------------------------------------------------ */

int CloseFile(int fd) {

	ASSERT( fd >= 0);

#ifdef DEBUG
	printf("CLOSE: FD=%d\n", fd);
#endif

	/*****************************/
	/* Check for Commit or Abort */
	/*****************************/

	int commitStatus = c->commit(fd, true);

	return commitStatus;
}

/* ------------------------------------------------------------------ */
