/****************/
/* Oleksandr Diachenko	*/
/* Date		*/
/* CS 244B	*/
/* Spring 2016	*/
/****************/

#define DEBUG

#include <stdio.h>

#include <server.h>
#include <unistd.h>
#include <fcntl.h>

#include <iostream>
#include <fstream>

#include <map>

#include <string.h>
#include <sstream>

#include <sys/stat.h>

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

		this->prepareLocalFileSystem();

		while (1) {

			char body[MAX_PACKET_SIZE] = { };
			int bytes = this->network->readFromSocket(body, 0);
			if (bytes > 0) {
				DfsPacket *packet = new DfsPacket(body, bytes);
				this->processPacket(packet);
			}
		}

	}

	void prepareLocalFileSystem() {
		struct stat sb;

		if (stat(this->mountPoint.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) {

			exitError("machine already in use");

		}

		int createStatus = mkdir(this->mountPoint.c_str(), S_IRWXU | S_IRWXG);
	}

	string getMountPoint() {
		return this->mountPoint;
	}

private:

	string mountPoint;
	int packetLoss;
	Network *network;
	map<string, map<uint8_t, int> > transactionIdPerClient;
	map<int, string> fileNamePerFileDescriptor;
	uint8_t _transactionId;

	uint8_t beginTransaction(string clientId, string fileName) {

		string uncommittedFileName = this->getUncommittedFileName(
				this->_transactionId, clientId);
		string uncommittedFileNameAbsolute = this->getAbsolutePath(
				uncommittedFileName);

		int fd = open(uncommittedFileNameAbsolute.c_str(), O_WRONLY | O_CREAT,
				S_IRWXU | S_IRWXG);

		this->copyContent(uncommittedFileNameAbsolute,
				this->getAbsolutePath(fileName));

		transactionIdPerClient[clientId][this->_transactionId] = fd;
		fileNamePerFileDescriptor[fd] = fileName;

		BeginTransactionResponseEvent *beginTransactionEvent =
				new BeginTransactionResponseEvent(clientId, this->getServerId(),
						this->_transactionId);
		this->network->sendPacket(beginTransactionEvent, false);

		printf("OPENFILE: %s, fd: %d, transactionId: %d\n", fileName.c_str(),
				fd, this->_transactionId);

		return this->_transactionId++;
	}

	void writeBlock(uint8_t tranId, string clientId, string serverId,
			uint8_t offset, uint8_t blockSize, char* bytes) {

		if (this->transactionIdPerClient[clientId].count(tranId) == 0) {
			printf("Invalid transaction, id: %d\n", tranId);
			return;
		}

		int fd = this->transactionIdPerClient[clientId][tranId];
		printf(
				"WRITE BLOCK to file descriptor: %d, clientId: %s, transactionId: %d, size: %d, offset: %d\n",
				fd, clientId.c_str(), tranId, blockSize, offset);

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

		char hostname[1024];
		hostname[1023] = '\0';
		gethostname(hostname, 1023);

		std::ostringstream ss;
		ss << ::getppid() << hostname;
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
						((CommitEvent *) event)->getSenderNodeId(),
						((CommitEvent *) event)->getCloseFile());
				break;
			case EVENT_TYPE_ROLLBACK:
				this->rollback(((RollbackEvent *) event)->getTransactionId(),
						((RollbackEvent *) event)->getSenderNodeId());
				break;
			default:
				break;
			};
		} else {
		}
	}

	void commitRequest(uint8_t transactionId, string clientId) {

		uint8_t readyToCommit = validateTransaction(transactionId, clientId);

		CommitVoteEvent *event = new CommitVoteEvent(clientId,
				this->getServerId(), transactionId, readyToCommit);

		this->network->sendPacket(event, true);

	}

	void finishCommit(uint8_t transactionId, string clientId, bool closeFile) {

		int commitTransactionStatus = commitTransaction(transactionId, clientId,
				closeFile);

		if (commitTransactionStatus == NormalReturn) {

			CommitRollbackAckEvent *event = new CommitRollbackAckEvent(clientId,
					this->getServerId(), transactionId);

			this->network->sendPacket(event, true);

		} else {
			printf("Unable to finish commit for transactionId: %d\n",
					transactionId);
		}

	}

	void rollback(uint8_t transactionId, string clientId) {
		printf("ROLLBACK transaction id : %d\n", transactionId);

		int fd = this->transactionIdPerClient[clientId][transactionId];

		string uncommittedFileName = this->getUncommittedFileName(transactionId,
				clientId);
		string uncommittedFileNameAbsolute = this->getAbsolutePath(
				uncommittedFileName);
		string fileName = fileNamePerFileDescriptor[fd];

		this->copyContent(uncommittedFileNameAbsolute,
				this->getAbsolutePath(fileName));

		CommitRollbackAckEvent *event = new CommitRollbackAckEvent(clientId,
				this->getServerId(), transactionId);

		this->network->sendPacket(event, true);

	}

	uint8_t validateTransaction(uint8_t transactionId, string clientId) {
		return POSITIVE_VOTE;
	}

	uint8_t commitTransaction(uint8_t transactionId, string clientId,
			bool closeFile) {

		printf("COMMIT transaction id : %d\n", transactionId);
		if (closeFile)
			printf("CLOSEFILE\n");

		int fd = this->transactionIdPerClient[clientId][transactionId];

		if (closeFile) {
			//Close file descriptor
			int closeStatus = close(fd);

			if (closeStatus != 0) {
				printf("Unable to close file, probably already closed\n");
				return ErrorReturn;
			}
			flushChanges(fd, transactionId, clientId);

			//Delete info about transaction
			transactionIdPerClient[clientId].erase(transactionId);
			fileNamePerFileDescriptor.erase(fd);

		} else
			flushChanges(fd, transactionId, clientId);

		return NormalReturn;
	}

	void flushChanges(int fd, uint8_t transactionId, string clientId) {

		//Flush changes to filesystem
		string absoluteFileName = this->getAbsolutePath(
				fileNamePerFileDescriptor[fd]);
		string absoluteUncommittedFileName = this->getAbsolutePath(
				this->getUncommittedFileName(transactionId, clientId));

		this->copyContent(absoluteFileName, absoluteUncommittedFileName);

		chmod(absoluteFileName.c_str(), S_IRWXU | S_IRWXG | S_IROTH);
	}

	string getUncommittedFileName(uint8_t transactionId, string clientId) {

		ostringstream ss;
		ss << ".unstaged" << (int) transactionId << getServerId();
		string uncommitedFileName = ss.str();

		return uncommitedFileName;
	}

	string getAbsolutePath(string relativeFileName) {

		return this->getMountPoint() + "//" + relativeFileName.c_str();

	}

	void copyContent(string destFileName, string sourceFileName) {

		ifstream sourceFile(sourceFileName.c_str(), std::ios::binary);
		ofstream destFile(destFileName.c_str(), std::ios::binary);

		destFile << sourceFile.rdbuf();

		destFile.close();
		sourceFile.close();

	}
};

int main(int argc, char* argv[]) {

	string port_option, mount_option, drop_option;
	unsigned short portNum;
	int packetLoss;

	for (int i = 0; i < argc; i++) {
		if (strcmp(argv[i], "-port") == 0)
			port_option = argv[i + 1];

		if (strcmp(argv[i], "-mount") == 0)
			mount_option = argv[i + 1];

		if (strcmp(argv[i], "-drop") == 0)
			drop_option = argv[i + 1];
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
