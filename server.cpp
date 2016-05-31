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
	map<uint8_t, string> fileNamePerFileDescriptor;
	uint8_t transactionId;

	uint8_t beginTransaction(string clientId, string fileName) {

		string uncommittedFileName = this->getUncommittedFileName(
				this->transactionId, clientId);
		string uncommittedFileNameAbsolute = this->getAbsolutePath(
				uncommittedFileName);

		int fd = open(uncommittedFileNameAbsolute.c_str(), O_WRONLY | O_CREAT,
				S_IRUSR | S_IWUSR);

		transactionIdPerClient[clientId][this->transactionId] = fd;
		fileNamePerFileDescriptor[fd] = fileName;

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
				this->finishCommit(((CommitEvent *) event)->getTransactionId(),
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

		int fd = this->transactionIdPerClient[clientId][transactionId];

		int closeStatus = close(fd);

		if (closeStatus != 0)
			return ErrorReturn;

		string absoluteFileName = this->getAbsolutePath(
				fileNamePerFileDescriptor[fd]);
		string absoluteUncommittedFileName = this->getAbsolutePath(
				this->getUncommittedFileName(transactionId, clientId));

		int renameStatus = rename(absoluteUncommittedFileName.c_str(),
				absoluteFileName.c_str());

		if (renameStatus == 0)
			return NormalReturn;
		else
			return ErrorReturn;

	}

	string getUncommittedFileName(uint8_t transactionId, string clientId) {

		ostringstream ss;
		ss << ".unstaged" << (int) transactionId << "file";
		string uncommitedFileName = ss.str();

		return uncommitedFileName;
	}

	string getAbsolutePath(string relativeFileName) {

		return this->getMountPoint() + "//" + relativeFileName.c_str();

	}
};

int main(int argc, char* argv[]) {

	string port_option, mount_option, drop_option;
	unsigned short portNum;
	int packetLoss;

	for (int i =0; i < argc; i++){
		if (strcmp(argv[i],"-port") == 0)
			port_option = argv[i+1];

		if (strcmp(argv[i],"-mount") == 0)
			mount_option = argv[i+1];

		if (strcmp(argv[i],"-drop") == 0)
			drop_option = argv[i+1];
	}

	if (port_option.empty())
		exitError("Please provide port value");
	if (mount_option.empty())
		exitError("Please provide mount value");
	if (drop_option.empty())
		exitError("Please provide drop value");

	portNum = atoi(port_option.c_str());
	packetLoss = atoi(drop_option.c_str());

	Network * n = new Network(portNum, packetLoss);
	Server * s = new Server(mount_option, packetLoss, n);

	n->netInit();

	s->runServer();

	return (NormalReturn);
}

/* ------------------------------------------------------------------ */
