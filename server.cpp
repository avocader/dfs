/****************/
/* Oleksandr Diachenko	*/
/* Date		*/
/* CS 244B	*/
/* Spring 2016	*/
/****************/

#define DEBUG

#include <stdio.h>
#include <getopt.h>

#include <server.h>
#include <unistd.h>
#include <fcntl.h>

#include <iostream>
#include <map>

#include <string.h>
#include <sstream>

/* ------------------------------------------------------------------ */

class Server {
public:

	Server() {
	}
	;

	Server(string mountPoint, int packetLoss, Network *network) {
		this->mountPoint = mountPoint;
		this->packetLoss = packetLoss;
		this->network = network;
	}

	void runServer() {

		while (1) {

			char body[256] = { };
			int bytes = this->network->readFromSocket(body, 0);
			if (bytes > 0) {
				DfsPacket *packet = new DfsPacket(body, bytes);
				this->processPacket(packet);
			}
		}

	}

	string getMountPoint() {
		return this->mountPoint;
	}

private:

	string mountPoint;
	int packetLoss;
	Network *network;
	map<string, map<uint8_t, int> > transactionIdPerClient;
	uint8_t transactionId;

	uint8_t beginTransaction(string clientId, string fileName) {

		int fd = open((this->getMountPoint() + "//" + fileName).c_str(),
				O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);

		transactionIdPerClient[clientId][this->transactionId] = fd;

		BeginTransactionResponseEvent *beginTransactionEvent =
				new BeginTransactionResponseEvent(clientId, this->getServerId(),
						transactionId);
		this->network->sendPacket(beginTransactionEvent);

		return transactionId++;
	}

	void writeBlock(uint8_t tranId, string clientId, string serverId,
			uint8_t offset, uint8_t blockSize, char* bytes) {

		int fd = this->transactionIdPerClient[clientId][tranId];
		printf("Writing block to file descriptor: %d\n", fd);

		if (lseek(fd, offset, SEEK_SET) < 0) {
			perror("WriteBlock Seek");
		}

		if ((write(fd, bytes, blockSize)) != blockSize) {
			perror("WriteBlock write");
		}

	}

	void processPacket(DfsPacket *packet) {

		BaseEvent *event = this->network->packetToEvent(packet);

		handleEvent(event);

	}

	string getServerId() {

		std::ostringstream ss;
		ss << ::getppid();
		string serverId = ss.str();

		return serverId;
	}

	void handleEvent(BaseEvent *event) {

		uint8_t eventType = event->getEventType();

		if (event->getReceiverNodeId() == this->getServerId()
				|| event->isBroadcast())

				{
			switch (eventType) {
			case EVENT_TYPE_BEGIN_TRANSACTION_REQUEST:
				uint8_t transactionId;
				transactionId =
						beginTransaction(event->getSenderNodeId(),
								((BeginTransactionRequestEvent *) event)->getFileName());
				printf("Started transactionId: %d\n", transactionId);
				break;
			case EVENT_TYPE_WRITE_BLOCK:

				this->writeBlock(
						((WriteBlockEvent *) event)->getTransactionId(),
						((WriteBlockEvent *) event)->getSenderNodeId(),
						((WriteBlockEvent *) event)->getReceiverNodeId(),
						((WriteBlockEvent *) event)->getOffset(),
						((WriteBlockEvent *) event)->getBlockSize(),
						((WriteBlockEvent *) event)->getBytes());
				break;

			case EVENT_TYPE_COMMIT_REQUEST:

				this->commitRequest(
						((CommitRequestEvent *) event)->getTransactionId(),
						((CommitRequestEvent *) event)->getSenderNodeId());

				break;

			case EVENT_TYPE_COMMIT:
				this->finishCommit(
						((CommitEvent *) event)->getTransactionId(),
						((CommitEvent *) event)->getSenderNodeId());
				break;
			default:
				printf("Ignoring event: %d\n", eventType);
				break;
			};
		} else {
		}
	}

	void commitRequest(uint8_t transactionId, string clientId) {

		uint8_t readyToCommit = validateTransaction(transactionId, clientId);

		CommitVoteEvent *event = new CommitVoteEvent(clientId,
				this->getServerId(), transactionId, readyToCommit);

		this->network->sendPacket(event);

	}

	void finishCommit(uint8_t transactionId, string clientId) {

		int commitTransactionStatus = commitTransaction(transactionId,
				clientId);

		if (commitTransactionStatus == NormalReturn) {

			CommitRollbackAckEvent *event = new CommitRollbackAckEvent(clientId,
					this->getServerId(), transactionId);

			this->network->sendPacket(event);


		}

	}

	uint8_t validateTransaction(uint8_t transactionId, string clientId) {
		return POSITIVE_VOTE;
	}

	uint8_t commitTransaction(uint8_t transactionId, string clientId) {
		return NormalReturn;

	}
};

int main(int argc, char* argv[]) {

	string port_option, mount_option, drop_option;
	unsigned short portNum;
	int packetLoss;

	int c;

	while (1) {
		static struct option long_options[] = { { "port", required_argument, 0,
				'p' }, { "mount", required_argument, 0, 'm' }, { "drop",
				required_argument, 0, 'd' }, { 0, 0, 0, 0 } };
		int option_index = 0;

		c = getopt_long(argc, argv, "port:mount:drop:", long_options,
				&option_index);

		if (c == -1)
			break;

		switch (c) {
		case 'p':
			port_option = optarg;
			break;
		case 'm':
			mount_option = optarg;
			break;
		case 'd':
			drop_option = optarg;
			break;

		default:
			exitError("Cannot parse input parameters.\n");
			break;
		}
	}

	portNum = atoi(port_option.c_str());
	packetLoss = atoi(drop_option.c_str());

	Network * n = new Network(portNum, packetLoss);
	Server * s = new Server(mount_option, packetLoss, n);

	n->netInit();

	s->runServer();

	return (NormalReturn);
}

/* ------------------------------------------------------------------ */
