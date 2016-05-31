/****************/
/* Oleksandr Diachenko	*/
/* 05/22/2016		*/
/* CS 244B	*/
/* Spring 2016	*/
/****************/

#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <list>
#include <set>

using namespace std;

#define DFS_PORT	44005
#define DFSGROUP	0xe0010101

#define FILENAMESIZE 128

#define TIMEOUT_READ_FROM_SOCKET_SEC 1
#define RETRY_ATTEMPTS_TIMES 30
#define CLOCKS_TIME_WAITING 100

#define EVENT_TYPE_BEGIN_TRANSACTION_REQUEST 0
#define EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE 1
#define EVENT_TYPE_WRITE_BLOCK 2
#define EVENT_TYPE_COMMIT_REQUEST 3
#define EVENT_TYPE_COMMIT_VOTE 4
#define EVENT_TYPE_COMMIT 5
#define EVENT_TYPE_ROLLBACK 6
#define EVENT_TYPE_COMMIT_ROLLBACK_ACK 7

#define NEGATIVE_VOTE 0
#define POSITIVE_VOTE 1

void exitError(string msg) {
	printf("%s\n", msg.c_str());
	exit(1);
}

class DfsPacket {

public:

	DfsPacket() {
	}

	DfsPacket(char body[], int len) {

		memcpy(this->body, body, len);
		this->len = len;
	}

	char body[256];
	uint8_t len;

};

class BaseEvent {

public:
	virtual DfsPacket* toPacket(void) = 0;
	string getSenderNodeId() {
		return this->senderNodeId;
	}

	string getReceiverNodeId() {
		return this->receiverNodeId;
	}

	uint8_t getEventType() {
		return this->eventType;
	}

	bool isBroadcast() {
		return this->broadcast;
	}

protected:
	uint8_t eventType;
	string senderNodeId;
	string receiverNodeId;
	bool broadcast = false;
};

class BeginTransactionRequestEvent: public BaseEvent {

public:
	BeginTransactionRequestEvent() {
	}
	BeginTransactionRequestEvent(string clientId, string fileName) {
		this->broadcast = true;
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_REQUEST;
		this->fileName = string(fileName);
		this->senderNodeId = clientId;
	}
	BeginTransactionRequestEvent(DfsPacket *packet) {
		this->broadcast = true;
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_REQUEST;
		this->fileName = string(packet->body + 1, FILENAMESIZE);
		this->senderNodeId = string(packet->body + 2 + FILENAMESIZE);
	}

	virtual DfsPacket* toPacket(void) {
		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//File name
		strncpy(pack->body + 1, this->fileName.c_str(), FILENAMESIZE);
		//Client id length
		pack->body[1 + FILENAMESIZE] = this->senderNodeId.length();
		//Client id
		strcpy(pack->body + 2 + FILENAMESIZE, this->senderNodeId.c_str());

		pack->len = 2 + FILENAMESIZE + this->senderNodeId.length();

		return pack;
	}

	string getFileName() {
		return this->fileName;
	}

protected:
	string fileName;
};

class BeginTransactionResponseEvent: public BaseEvent {

public:
	BeginTransactionResponseEvent() {
	}
	BeginTransactionResponseEvent(string clientId, string serverId,
			uint8_t transactionId) {
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE;
		this->receiverNodeId = clientId;
		this->senderNodeId = serverId;
		this->transactionId = transactionId;

	}
	BeginTransactionResponseEvent(DfsPacket *packet) {
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE;
		int clientIdLength = packet->body[1];
		this->receiverNodeId = string(packet->body + 2, clientIdLength);
		int serverIdLength = packet->body[2 + clientIdLength];
		this->senderNodeId = string(packet->body + 3 + clientIdLength,
				serverIdLength);
		this->transactionId = packet->body[3 + clientIdLength + serverIdLength];

	}

	virtual DfsPacket* toPacket(void) {
		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Client id length
		pack->body[1] = this->receiverNodeId.length();
		//Client id
		strcpy(pack->body + 2, this->receiverNodeId.c_str());
		//Server id length
		pack->body[2 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		strcpy(pack->body + 3 + this->receiverNodeId.length(),
				this->senderNodeId.c_str());
		//Transaction id
		pack->body[3 + this->receiverNodeId.length()
				+ this->senderNodeId.length()] = this->transactionId;

		pack->len = 4 + this->receiverNodeId.length()
				+ this->senderNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

protected:
	uint8_t transactionId;
};

class WriteBlockEvent: public BaseEvent {

public:
	WriteBlockEvent() {
	}

	WriteBlockEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_WRITE_BLOCK;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->senderNodeId = string(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = string(packet->body + 4 + clientIdLength,
				serverIdLength);
		this->offset = packet->body[4 + clientIdLength + serverIdLength];
		this->blockSize = packet->body[5 + clientIdLength + serverIdLength];
		this->bytes = packet->body + 6 + clientIdLength + serverIdLength;

	}

	WriteBlockEvent(string clientId, string serverId, uint8_t transactionId,
			char * bytes, int offset, int blockSize) {

		this->eventType = EVENT_TYPE_WRITE_BLOCK;
		this->senderNodeId = clientId;
		this->receiverNodeId = serverId;
		this->transactionId = transactionId;
		this->bytes = bytes;
		this->offset = offset;
		this->blockSize = blockSize;
	}

	virtual DfsPacket* toPacket(void) {
		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Transaction id
		pack->body[1] = this->transactionId;
		//Client id length
		pack->body[2] = this->senderNodeId.length();
		//Client id
		strcpy(pack->body + 3, this->senderNodeId.c_str());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		strcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str());
		//Offset
		pack->body[4 + this->receiverNodeId.length()
				+ this->senderNodeId.length()] = this->offset;
		//Block size
		pack->body[5 + this->receiverNodeId.length()
				+ this->senderNodeId.length()] = this->blockSize;
		//Bytes
		std::memcpy(
				pack->body + 6 + this->receiverNodeId.length()
						+ this->senderNodeId.length(), bytes, blockSize);
		pack->len = 6 + this->receiverNodeId.length()
				+ this->senderNodeId.length() + blockSize;

		return pack;
	}

	uint8_t getBlockSize() {
		return this->blockSize;
	}
	char* getBytes() {
		return this->bytes;
	}
	uint8_t getOffset() {
		return this->offset;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

protected:
	uint8_t transactionId;
	uint8_t offset;
	uint8_t blockSize;
	char *bytes;

};

class CommitRequestEvent: public BaseEvent {

public:
	CommitRequestEvent() {
	}

	CommitRequestEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_COMMIT_REQUEST;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->senderNodeId = string(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = string(packet->body + 4 + clientIdLength,
				serverIdLength);

	}

	CommitRequestEvent(string clientId, string serverId,
			uint8_t transactionId) {

		this->eventType = EVENT_TYPE_COMMIT_REQUEST;
		this->senderNodeId = clientId;
		this->receiverNodeId = serverId;
		this->transactionId = transactionId;
	}

	virtual DfsPacket* toPacket(void) {

		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Transaction id
		pack->body[1] = this->transactionId;
		//Client id length
		pack->body[2] = this->senderNodeId.length();
		//Client id
		strcpy(pack->body + 3, this->senderNodeId.c_str());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		strcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str());

		pack->len = 4 + this->senderNodeId.length()
				+ this->receiverNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

protected:
	uint8_t transactionId;

};

class CommitVoteEvent: public BaseEvent {

public:

	CommitVoteEvent() {
	}

	CommitVoteEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_COMMIT_VOTE;
		this->transactionId = packet->body[1];
		this->vote = packet->body[2];
		int clientIdLength = packet->body[3];
		this->receiverNodeId = string(packet->body + 4, clientIdLength);
		int serverIdLength = packet->body[4 + clientIdLength];
		this->senderNodeId = string(packet->body + 5 + clientIdLength,
				serverIdLength);

	}

	CommitVoteEvent(string clientId, string serverId, uint8_t transactionId,
			uint8_t vote) {

		this->eventType = EVENT_TYPE_COMMIT_VOTE;
		this->senderNodeId = serverId;
		this->receiverNodeId = clientId;
		this->transactionId = transactionId;
		this->vote = vote;
	}

	virtual DfsPacket* toPacket(void) {

		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Transaction id
		pack->body[1] = this->transactionId;
		pack->body[2] = this->vote;
		//Client id length
		pack->body[3] = this->receiverNodeId.length();
		//Client id
		strcpy(pack->body + 4, this->receiverNodeId.c_str());
		//Server id length
		pack->body[4 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		strcpy(pack->body + 5 + this->receiverNodeId.length(),
				this->senderNodeId.c_str());

		pack->len = 5 + this->receiverNodeId.length()
				+ this->senderNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

	uint8_t getVote() {
		return this->vote;
	}

protected:
	uint8_t transactionId;
	uint8_t vote;

};

class CommitEvent: public BaseEvent {

public:
	CommitEvent() {
	}

	CommitEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_COMMIT;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->senderNodeId = string(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = string(packet->body + 4 + clientIdLength,
				serverIdLength);
		this->closeFile = packet->body[4 + clientIdLength + serverIdLength];
	}

	CommitEvent(string clientId, string serverId, uint8_t transactionId, uint8_t closeFile) {

		this->eventType = EVENT_TYPE_COMMIT;
		this->senderNodeId = clientId;
		this->receiverNodeId = serverId;
		this->transactionId = transactionId;
		this->closeFile = closeFile;

	}

	virtual DfsPacket* toPacket(void) {

		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Transaction id
		pack->body[1] = this->transactionId;
		//Client id length
		pack->body[2] = this->senderNodeId.length();
		//Client id
		strcpy(pack->body + 3, this->senderNodeId.c_str());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		strcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str());
		//Close file
		pack->body[4 + this->senderNodeId.length() + this->receiverNodeId.length()] = this->closeFile;

		pack->len = 5 + this->senderNodeId.length()
				+ this->receiverNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

	uint8_t getCloseFile() {
		return this->closeFile;
	}

protected:
	uint8_t transactionId;
	uint8_t closeFile;

};

class CommitRollbackAckEvent: public BaseEvent {

public:
	CommitRollbackAckEvent() {
	}

	CommitRollbackAckEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_COMMIT_ROLLBACK_ACK;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->receiverNodeId = string(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->senderNodeId = string(packet->body + 4 + clientIdLength,
				serverIdLength);

	}

	CommitRollbackAckEvent(string clientId, string serverId,
			uint8_t transactionId) {

		this->eventType = EVENT_TYPE_COMMIT_ROLLBACK_ACK;
		this->senderNodeId = serverId;
		this->receiverNodeId = clientId;
		this->transactionId = transactionId;
	}

	virtual DfsPacket* toPacket(void) {

		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Transaction id
		pack->body[1] = this->transactionId;
		//Client id length
		pack->body[2] = this->receiverNodeId.length();
		//Client id
		strcpy(pack->body + 3, this->receiverNodeId.c_str());
		//Server id length
		pack->body[3 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		strcpy(pack->body + 4 + this->receiverNodeId.length(),
				this->senderNodeId.c_str());

		pack->len = 4 + this->receiverNodeId.length()
				+ this->senderNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

protected:
	uint8_t transactionId;

};

class Network {

private:
	int s;
	unsigned short portNum;
	int packetLoss;
	struct sockaddr_in groupAddr;
	struct sockaddr_in nullAddr;

public:
	Network(unsigned short portNum, int packetLoss) {
		this->portNum = portNum;
		this->packetLoss = packetLoss;
	}

	void netInit() {

		struct ip_mreq mreq;
		this->s = socket(AF_INET, SOCK_DGRAM, 0);
		if (this->s < 0)
			exitError("can't get socket");

		int reuse = 1;
		if (setsockopt(this->s, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse))
				< 0) {
			exitError("setsockopt failed (SO_REUSEADDR)");
		}

		memset(&nullAddr, 0, sizeof(nullAddr));
		nullAddr.sin_family = AF_INET;
		nullAddr.sin_addr.s_addr = htonl(INADDR_ANY);
		nullAddr.sin_port = htons(this->portNum);
		if (bind(this->s, (struct sockaddr *) &nullAddr, sizeof(nullAddr)) < 0)
			exitError("Unable to bind scoket");

		int ttl = 1;
		if (setsockopt(this->s, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl))
				< 0) {
			exitError("setsockopt failed (IP_MULTICAST_TTL)");
		}

		/* join the multicast group */
		mreq.imr_multiaddr.s_addr = htonl(DFSGROUP);
		mreq.imr_interface.s_addr = htonl(INADDR_ANY);

		if (setsockopt(this->s, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *) &mreq,
				sizeof(mreq)) < 0) {
			exitError("setsockopt failed (IP_ADD_MEMBERSHIP)");
		}

		memcpy(&groupAddr, &nullAddr, sizeof(struct sockaddr_in));
		groupAddr.sin_addr.s_addr = htonl(DFSGROUP);

	}

	BaseEvent* packetToEvent(DfsPacket *packet) {
		uint8_t eventType = packet->body[0];
		BaseEvent *event;

		switch (eventType) {
		case EVENT_TYPE_BEGIN_TRANSACTION_REQUEST:
			event = new BeginTransactionRequestEvent(packet);
			break;
		case EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE:
			event = new BeginTransactionResponseEvent(packet);
			break;
		case EVENT_TYPE_WRITE_BLOCK:
			event = new WriteBlockEvent(packet);
			break;
		case EVENT_TYPE_COMMIT_REQUEST:
			event = new CommitRequestEvent(packet);
			break;
		case EVENT_TYPE_COMMIT_VOTE:
			event = new CommitVoteEvent(packet);
			break;
		case EVENT_TYPE_COMMIT:
			event = new CommitEvent(packet);
			break;
		case EVENT_TYPE_COMMIT_ROLLBACK_ACK:
			event = new CommitRollbackAckEvent(packet);
			break;
		default:
			printf("Unknown packet type\n");
			break;
		};

		return event;
	}

	void sendPacket(DfsPacket *pack) {

		if (sendto((int) this->s, pack->body, pack->len, 0,
				(struct sockaddr *) &groupAddr, sizeof(groupAddr)) < 0) {
			exitError("Cannot send a packet.\n");
		};

	}

	void sendPacket(BaseEvent *event) {
		sendPacket(event->toPacket());
	}

	list<DfsPacket*> sendPacketRetry(string expectedReceiverNodeId,
			int numberOfNodesExpected, BaseEvent *eventToSend,
			int expectedEventType) {

		list<DfsPacket*> packets;
		set < string > receivedNodesIds;

		for (int i = 0; i < RETRY_ATTEMPTS_TIMES; i++) {

			printf(
					"Attempt number %d, number responses received: %d, expected to receive: %d\n",
					i + 1, receivedNodesIds.size(), numberOfNodesExpected);

			this->sendPacket(eventToSend);

			clock_t start_time = clock();

			BaseEvent *receivedEvent;

			while (receivedNodesIds.size() < numberOfNodesExpected) {
				char body[256] = { };
				int bytes = this->readFromSocket(body, 0);
				if (bytes > 0) {
					DfsPacket *packet = new DfsPacket(body, bytes);

					receivedEvent = this->packetToEvent(packet);

					if (receivedEvent->getReceiverNodeId()
							== expectedReceiverNodeId
							&& receivedEvent->getEventType()
									== expectedEventType) {

						if (receivedNodesIds.count(
								receivedEvent->getSenderNodeId()) == 0) {
							packets.push_front(packet);
							receivedNodesIds.insert(
									receivedEvent->getSenderNodeId());
						}
					}
				}

				if (clock() - start_time > CLOCKS_TIME_WAITING)
					break;

			}

			if (receivedNodesIds.size() == numberOfNodesExpected)
				break;

		}
		return packets;
	}

	int readFromSocket(char *buf, sockaddr *addr) {

		fd_set fdmask;
		int ret, bytes = 0;
		struct timeval timeout = { TIMEOUT_READ_FROM_SOCKET_SEC, 0 };
		socklen_t fromLen = sizeof(addr);
		FD_ZERO(&fdmask);
		FD_SET(this->s, &fdmask);

		ret = select(32, &fdmask, NULL, NULL, &timeout);

		if (ret == 1 && FD_ISSET(this->s, &fdmask)) {

			bytes = recvfrom(this->s, buf, sizeof(DfsPacket), 0, addr,
					&fromLen);
		}

		return bytes;
	}
};

