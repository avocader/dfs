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
#include <string>
#include <list>
#include <set>
#include <cmath>
#include <unistd.h>
#include <sys/time.h>

using namespace std;

//Multicast group
#define DFSGROUP	0xe0010101

//Max packet length
#define MAX_PACKET_SIZE 65507

#define FILENAMESIZE 128

//Timing constants
#define TIMEOUT_READ_FROM_SOCKET_SEC 1
#define RETRY_ATTEMPTS_TIMES 30
#define RETRY_WAIT_SEC 1
#define MSECONDS_TO_SLEEP_BETWEEN_PACKETS 10000

//Events
#define EVENT_TYPE_BEGIN_TRANSACTION_REQUEST 0
#define EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE 1
#define EVENT_TYPE_WRITE_BLOCK 2
#define EVENT_TYPE_COMMIT_REQUEST 3
#define EVENT_TYPE_COMMIT_VOTE 4
#define EVENT_TYPE_COMMIT 5
#define EVENT_TYPE_ROLLBACK 6
#define EVENT_TYPE_COMMIT_ROLLBACK_ACK 7

//Return codes
#define RC_SUCCESS 0
#define RC_FILE_ALREADY_OPENED 1

//Commit votes
#define NEGATIVE_VOTE 0
#define POSITIVE_VOTE 1

//Desired probability of error in system
#define SYSTEM_RELIABILITY 1e-6

//
#define NO_TRAN_ID 0

void exitError(string msg) {
	printf("%s\n", msg.c_str());
	exit(1);
}

void intToBytes(int paramInt, uint8_t *bytes) {

	static uint8_t arrayOfByte[4];

	for (int i = 0; i < 4; i++) {

		bytes[3 - i] = (paramInt >> 8 * i);

	}

}

int bytesToInt(uint8_t * bytes) {

	int intValue = 0;
	intValue = (bytes[3]) | (bytes[2] << 8) | (bytes[1] << 16)
			| (bytes[0] << 24);

	return intValue;

}

string createString(uint8_t * bytes, int len) {

	string result = string(bytes, bytes + len);

	return result;

}

class DfsPacket {

public:

	DfsPacket() {
	}

	DfsPacket(char body[], int len) {

		memcpy(this->body, body, len);
		this->len = len;
	}

	uint8_t body[MAX_PACKET_SIZE];
	int getLen() {
		return this->len;
	}

public:
	int len;

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
		this->fileName = fileName;
		this->senderNodeId = clientId;
	}
	BeginTransactionRequestEvent(DfsPacket *packet) {
		this->broadcast = true;
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_REQUEST;
		this->fileName = createString(packet->body + 1, FILENAMESIZE);
		int clientIdLength = packet->body[1 + FILENAMESIZE];
		this->senderNodeId = createString(packet->body + 2 + FILENAMESIZE,
				clientIdLength);
	}

	virtual DfsPacket* toPacket(void) {
		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//File name
		memcpy(pack->body + 1, this->fileName.c_str(), FILENAMESIZE);
		//Client id length
		pack->body[1 + FILENAMESIZE] = this->senderNodeId.length();
		//Client id
		memcpy(pack->body + 2 + FILENAMESIZE, this->senderNodeId.c_str(),
				this->senderNodeId.length());

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
			uint8_t transactionId, uint8_t returnCode) {
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE;
		this->receiverNodeId = clientId;
		this->senderNodeId = serverId;
		this->transactionId = transactionId;
		this->returnCode = returnCode;

	}
	BeginTransactionResponseEvent(DfsPacket *packet) {
		this->eventType = EVENT_TYPE_BEGIN_TRANSACTION_RESPONSE;
		int clientIdLength = packet->body[1];
		this->receiverNodeId = createString(packet->body + 2, clientIdLength);
		int serverIdLength = packet->body[2 + clientIdLength];
		this->senderNodeId = createString(packet->body + 3 + clientIdLength,
				serverIdLength);
		this->transactionId = packet->body[3 + clientIdLength + serverIdLength];
		this->returnCode = packet->body[4 + clientIdLength + serverIdLength];

	}

	virtual DfsPacket* toPacket(void) {
		DfsPacket *pack = new DfsPacket();

		//Event type
		pack->body[0] = this->eventType;
		//Client id length
		pack->body[1] = this->receiverNodeId.length();
		//Client id
		memcpy(pack->body + 2, this->receiverNodeId.c_str(),
				this->receiverNodeId.length());
		//Server id length
		pack->body[2 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		memcpy(pack->body + 3 + this->receiverNodeId.length(),
				this->senderNodeId.c_str(), this->senderNodeId.length());
		//Transaction id
		pack->body[3 + this->receiverNodeId.length()
				+ this->senderNodeId.length()] = this->transactionId;
		//Return code
		pack->body[4 + this->receiverNodeId.length()
				+ this->senderNodeId.length()] = returnCode;

		pack->len = 5 + this->receiverNodeId.length()
				+ this->senderNodeId.length();

		return pack;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

	uint8_t getReturnCode() {
		return this->returnCode;
	}

protected:
	uint8_t transactionId;
	uint8_t returnCode;

};

class WriteBlockEvent: public BaseEvent {

public:
	WriteBlockEvent() {
	}

	WriteBlockEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_WRITE_BLOCK;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];

		this->senderNodeId = createString(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = createString(packet->body + 4 + clientIdLength,
				serverIdLength);

		this->offset = bytesToInt(
				packet->body + 4 + this->senderNodeId.length()
						+ this->receiverNodeId.length());

		this->blockSize = bytesToInt(
				packet->body + 8 + this->receiverNodeId.length()
						+ this->senderNodeId.length());

		this->bytes = new char[this->blockSize];
		memcpy(this->bytes, packet->body + 12 + clientIdLength + serverIdLength,
				this->blockSize);

	}

	WriteBlockEvent(string clientId, string serverId, uint8_t transactionId,
			char * bytes, uint32_t offset, int blockSize) {

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
		memcpy(pack->body + 3, this->senderNodeId.c_str(),
				this->senderNodeId.length());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		memcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str(), this->receiverNodeId.length());
		//Offset
		intToBytes(this->offset,
				pack->body + 4 + this->senderNodeId.length()
						+ this->receiverNodeId.length());
		//Block size
		intToBytes(this->blockSize,
				pack->body + 8 + this->receiverNodeId.length()
						+ this->senderNodeId.length());
		//Bytes
		memcpy(
				pack->body + 12 + this->receiverNodeId.length()
						+ this->senderNodeId.length(), bytes, blockSize);

		pack->len = 12 + this->receiverNodeId.length()
				+ this->senderNodeId.length() + blockSize;

		return pack;
	}

	int getBlockSize() {
		return this->blockSize;
	}
	char* getBytes() {
		return this->bytes;
	}
	uint32_t getOffset() {
		return this->offset;
	}

	uint8_t getTransactionId() {
		return this->transactionId;
	}

protected:
	uint8_t transactionId;
	uint32_t offset;
	int blockSize;
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
		this->senderNodeId = createString(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = createString(packet->body + 4 + clientIdLength,
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
		memcpy(pack->body + 3, this->senderNodeId.c_str(),
				this->senderNodeId.length());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		memcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str(), this->receiverNodeId.length());

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
		this->receiverNodeId = createString(packet->body + 4, clientIdLength);
		int serverIdLength = packet->body[4 + clientIdLength];
		this->senderNodeId = createString(packet->body + 5 + clientIdLength,
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
		memcpy(pack->body + 4, this->receiverNodeId.c_str(),
				this->receiverNodeId.length());
		//Server id length
		pack->body[4 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		memcpy(pack->body + 5 + this->receiverNodeId.length(),
				this->senderNodeId.c_str(), this->senderNodeId.length());

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
		this->senderNodeId = createString(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = createString(packet->body + 4 + clientIdLength,
				serverIdLength);
		this->closeFile = packet->body[4 + clientIdLength + serverIdLength];
	}

	CommitEvent(string clientId, string serverId, uint8_t transactionId,
			uint8_t closeFile) {

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
		memcpy(pack->body + 3, this->senderNodeId.c_str(),
				this->senderNodeId.length());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		memcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str(), this->receiverNodeId.length());
		//Close file
		pack->body[4 + this->senderNodeId.length()
				+ this->receiverNodeId.length()] = this->closeFile;

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

class RollbackEvent: public BaseEvent {
public:
	RollbackEvent() {
	}

	RollbackEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_ROLLBACK;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->senderNodeId = createString(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->receiverNodeId = createString(packet->body + 4 + clientIdLength,
				serverIdLength);
	}

	RollbackEvent(string clientId, string serverId, uint8_t transactionId) {

		this->eventType = EVENT_TYPE_ROLLBACK;
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
		memcpy(pack->body + 3, this->senderNodeId.c_str(),
				this->senderNodeId.length());
		//Server id length
		pack->body[3 + this->senderNodeId.length()] =
				this->receiverNodeId.length();
		//Server id
		memcpy(pack->body + 4 + this->senderNodeId.length(),
				this->receiverNodeId.c_str(), this->receiverNodeId.length());

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

class CommitRollbackAckEvent: public BaseEvent {

public:
	CommitRollbackAckEvent() {
	}

	CommitRollbackAckEvent(DfsPacket *packet) {

		this->eventType = EVENT_TYPE_COMMIT_ROLLBACK_ACK;
		this->transactionId = packet->body[1];
		int clientIdLength = packet->body[2];
		this->receiverNodeId = createString(packet->body + 3, clientIdLength);
		int serverIdLength = packet->body[3 + clientIdLength];
		this->senderNodeId = createString(packet->body + 4 + clientIdLength,
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
		memcpy(pack->body + 3, this->receiverNodeId.c_str(),
				this->receiverNodeId.length());
		//Server id length
		pack->body[3 + this->receiverNodeId.length()] =
				this->senderNodeId.length();
		//Server id
		memcpy(pack->body + 4 + this->receiverNodeId.length(),
				this->senderNodeId.c_str(), this->senderNodeId.length());

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
	int packetsRead;
	int packetsDropped;

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
		case EVENT_TYPE_ROLLBACK:
			event = new RollbackEvent(packet);
			break;
		default:
			printf("Unknown packet type\n");
			break;
		};

		return event;
	}

	//Send packet to multicast group
	//param pack is packet pointer to send
	//param reliable if true try to send packet multiple times to provide given system reliability
	//param recepientsNum number of recepients supposed to receive message
	void sendPacket(DfsPacket *pack, bool reliable, int recepientsNum) {

		float iterationsNum = 1;
		if (reliable && this->packetLoss > 0) {
			iterationsNum = log(SYSTEM_RELIABILITY)
					/ log(this->packetLoss / 100.0)
					- log(recepientsNum) / log(this->packetLoss / 100.0);
		}
		for (int i = 0; i < iterationsNum; i++) {
			if (sendto((int) this->s, pack->body, pack->getLen(), 0,
					(struct sockaddr *) &groupAddr, sizeof(groupAddr)) < 0) {
				exitError("Cannot send a packet.\n");
			};
			usleep(MSECONDS_TO_SLEEP_BETWEEN_PACKETS);
		}

	}

	void sendPacket(BaseEvent *event, bool reliable, int recepientsNum) {
		sendPacket(event->toPacket(), reliable, recepientsNum);
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

			this->sendPacket(eventToSend, false, 1);

			struct timeval start_time, current_time;
			long seconds_elapsed;
			gettimeofday(&start_time, NULL);

			BaseEvent *receivedEvent;

			while (receivedNodesIds.size() < numberOfNodesExpected) {

				char body[MAX_PACKET_SIZE] = { };

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

				gettimeofday(&current_time, NULL);

				seconds_elapsed = current_time.tv_sec - start_time.tv_sec;

				if (seconds_elapsed > RETRY_WAIT_SEC)
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

		this->packetsRead++;
		if ((rand() % 100) < this->packetLoss) {
			this->packetsDropped++;
			return 0;
		}

		return bytes;
	}
};

