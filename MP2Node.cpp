/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
    this->transactionsMap = new map<int, Transaction *>;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

Transaction::Transaction(int id, int timestamp, MessageType msgType, string key, string value) {
    this->id = id;
    this->timestamp = timestamp;
    this->msgType = msgType;
    this->key = key;
    this->value = value;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change = false;

    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();
    curMemList.emplace_back(this->memberNode->addr);

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    change = curMemList.size() != this->ring.size();
    this->ring = curMemList;

    if (this->ht->currentSize() > 0 && change)
        this->stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    /*
     * Implement this
     */

    // Create elements that compose the message to send and the transaction
    int transactionId = g_transID++;
    Address address = this->memberNode->addr;
    MessageType msgType = MessageType::CREATE;

    // 1. Message Construction
    Message *msg = new Message(transactionId, address, msgType, key, value);
    string msgData = msg->toString();

    // 2. Find Replicas
    vector<Node> replicas = this->findNodes(key);

    // 3. Send message to replicas
    for (auto &replica : replicas) {
        int currentTimestamp = this->par->getcurrtime();

        Transaction *transaction = new Transaction(transactionId, currentTimestamp, msgType, key, value);
        this->transactionsMap->emplace(transactionId, transaction);

        Address *fromAddress = &address;
        Address *toAddress = replica.getAddress();

        this->emulNet->ENsend(fromAddress, toAddress, msgData);
    }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    /*
     * Implement this
     */
    // Create elements that compose the message to send and the transaction
    int transactionId = g_transID++;
    Address address = this->memberNode->addr;
    MessageType msgType = MessageType::READ;

    // 1. Message Construction
    Message *msg = new Message(transactionId, address, msgType, key);
    string msgData = msg->toString();

    // 2. Find Replicas
    vector<Node> replicas = this->findNodes(key);

    // 3. Send message to replicas
    for (auto &replica : replicas) {
        int currentTimestamp = this->par->getcurrtime();

        Transaction *transaction = new Transaction(transactionId, currentTimestamp, msgType, key);
        this->transactionsMap->emplace(transactionId, transaction);

        Address *fromAddress = &address;
        Address *toAddress = replica.getAddress();

        this->emulNet->ENsend(fromAddress, toAddress, msgData);
    }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    /*
     * Implement this
     */
    // Create elements that compose the message to send and the transaction
    int transactionId = g_transID++;
    Address address = this->memberNode->addr;
    MessageType msgType = MessageType::UPDATE;

    // 1. Message Construction
    Message *msg = new Message(transactionId, address, msgType, key, value);
    string msgData = msg->toString();

    // 2. Find Replicas
    vector<Node> replicas = this->findNodes(key);

    // 3. Send message to replicas
    for (auto &replica : replicas) {
        int currentTimestamp = this->par->getcurrtime();

        Transaction *transaction = new Transaction(transactionId, currentTimestamp, msgType, key, value);
        this->transactionsMap->emplace(transactionId, transaction);

        Address *fromAddress = &address;
        Address *toAddress = replica.getAddress();

        this->emulNet->ENsend(fromAddress, toAddress, msgData);
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    /*
     * Implement this
     */
    // Create elements that compose the message to send and the transaction
    int transactionId = g_transID++;
    Address address = this->memberNode->addr;
    MessageType msgType = MessageType::DELETE;

    // 1. Message Construction
    Message *msg = new Message(transactionId, address, msgType, key);
    string msgData = msg->toString();

    // 2. Find Replicas
    vector<Node> replicas = this->findNodes(key);

    // 3. Send message to replicas
    for (auto &replica : replicas) {
        int currentTimestamp = this->par->getcurrtime();

        Transaction *transaction = new Transaction(transactionId, currentTimestamp, msgType, key);
        this->transactionsMap->emplace(transactionId, transaction);

        Address *fromAddress = &address;
        Address *toAddress = replica.getAddress();

        this->emulNet->ENsend(fromAddress, toAddress, msgData);
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transId) {
    /*
     * Implement this
     */
    // Insert key, value, replicaType into the hash table
    bool createSuccess = this->ht->create(key, value);

    if (createSuccess)
        this->log->logCreateSuccess(&this->memberNode->addr, false, transId, key, value);
    else
        this->log->logCreateFail(&this->memberNode->addr, false, transId, key, value);

    return createSuccess;
//    bool success = false;
//    if(transId != -1){
//        success = this->ht->create(key, value);
//        if(success)
//            log->logCreateSuccess(&memberNode->addr, false, transId, key, value);
//        else
//            log->logCreateFail(&memberNode->addr, false, transId, key, value);
//    }else{
//        string content = this->ht->read(key);
//        bool exist = (content != "");
//        if(!exist){
//            success = this->ht->create(key, value);
//        }
//    }
//    return success;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transId) {
    /*
     * Implement this
     */
    // Read key from local hash table and return value
    string value = this->ht->read(key);
    bool readSuccess = !value.empty();

    if (readSuccess)
        this->log->logReadSuccess(&this->memberNode->addr, false, transId, key, value);
    else
        this->log->logReadFail(&this->memberNode->addr, false, transId, key);

    return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transId) {
    /*
     * Implement this
     */
    // Update key in local hash table and return true or false
    bool updateSucess = this->ht->update(key, value);

    if (updateSucess)
        this->log->logUpdateSuccess(&this->memberNode->addr, false, transId, key, value);
    else
        this->log->logUpdateFail(&this->memberNode->addr, false, transId, key, value);

    return updateSucess;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transId) {
    /*
     * Implement this
     */
    // Delete the key from the local hash table
    bool deleteSuccess = this->ht->deleteKey(key);

    if (deleteSuccess)
        this->log->logDeleteSuccess(&this->memberNode->addr, false, transId, key);
    else
        this->log->logDeleteFail(&this->memberNode->addr, false, transId, key);

    return deleteSuccess;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string messageString(data, data + size);

        /*
         * Handle the message types here
         */
        this->handleMessage(new Message(messageString));
    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
    this->analyzeQuorumConsistency();
}

void MP2Node::handleMessage(Message *msgReceived) {
//    if (msgReceived->type == MessageType::CREATE)
//        this->handleCreateMessage(msgReceived);
//
//    if (msgReceived->type == MessageType::READ)
//        this->handleReadMessage(msgReceived);
//
//    if (msgReceived->type == MessageType::UPDATE)
//        this->handleUpdateMessage(msgReceived);
//
//    if (msgReceived->type == MessageType::DELETE)
//        this->handleDeleteMessage(msgReceived);
//
//    if (msgReceived->type == MessageType::REPLY)
//        this->handleReplyMessage(msgReceived);
//
//    if (msgReceived->type == MessageType::READREPLY)
//        this->handleReadReplyMessage(msgReceived);

    Message msg = *msgReceived;

    switch (msg.type) {
        case MessageType::CREATE: {
            this->handleCreateMessage(msgReceived);
            break;
        }
        case MessageType::DELETE: {
            this->handleDeleteMessage(msgReceived);
            break;
        }
        case MessageType::READ: {
            this->handleReadMessage(msgReceived);
            break;
        }
        case MessageType::UPDATE: {
            this->handleUpdateMessage(msgReceived);
            break;
        }
        case MessageType::READREPLY: {
            this->handleReadReplyMessage(msgReceived);
            break;
        }
        case MessageType::REPLY: {
            this->handleReplyMessage(msgReceived);
            break;
        }
    }
}

void MP2Node::handleCreateMessage(Message *msgReceived) {
    bool createSuccess = this->createKeyValue(msgReceived->key, msgReceived->value, msgReceived->replica,
                                              msgReceived->transID);
    Message msg(msgReceived->transID, this->memberNode->addr, MessageType::REPLY, createSuccess);
//    string data = msg.toString();
    this->emulNet->ENsend(&this->memberNode->addr, &msgReceived->fromAddr, msg.toString());
}

void MP2Node::handleReadMessage(Message *msgReceived) {
    string readContent = this->readKey(msgReceived->key, msgReceived->transID);
    auto *replyMsg = new Message(msgReceived->transID, this->memberNode->addr, readContent);
    this->emulNet->ENsend(&this->memberNode->addr, &msgReceived->fromAddr, replyMsg->toString());
}

void MP2Node::handleUpdateMessage(Message *msgReceived) {
    bool updateSuccess = this->updateKeyValue(msgReceived->key, msgReceived->value, msgReceived->replica,
                                              msgReceived->transID);
    auto *replyMsg = new Message(msgReceived->transID, this->memberNode->addr, MessageType::REPLY, updateSuccess);
    this->emulNet->ENsend(&this->memberNode->addr, &msgReceived->fromAddr, replyMsg->toString());
}

void MP2Node::handleDeleteMessage(Message *msgReceived) {
    bool deleteSuccess = this->deletekey(msgReceived->key, msgReceived->transID);
    auto *replyMsg = new Message(msgReceived->transID, this->memberNode->addr, MessageType::REPLY, deleteSuccess);
    this->emulNet->ENsend(&this->memberNode->addr, &msgReceived->fromAddr, replyMsg->toString());
}

void MP2Node::handleReplyMessage(Message *msgReceived) {
    if (this->transactionsMap->find(msgReceived->transID) == this->transactionsMap->end())
        return;

    Transaction *transaction = (*transactionsMap)[msgReceived->transID];
    transaction->replyCount++;

    if (msgReceived->success)
        transaction->successCount++;
}

void MP2Node::handleReadReplyMessage(Message *msgReceived) {
    if (this->transactionsMap->find(msgReceived->transID) == this->transactionsMap->end())
        return;

    Transaction *transaction = (*transactionsMap)[msgReceived->transID];
    transaction->replyCount++;
    transaction->value = msgReceived->value;

    if (!msgReceived->value.empty()) // ACA ESTA EL PROBLEMA
        transaction->successCount++;
}


void MP2Node::analyzeQuorumConsistency() {
    for (auto transactionIterator = this->transactionsMap->begin();
         transactionIterator != this->transactionsMap->end();
         transactionIterator++) {
        Transaction *transaction = transactionIterator->second;

        // Values to analyze consistency
        int transactionReplyCount = transaction->replyCount;
        int transactionSuccessCount = transaction->successCount;
        int transactionTimestamp = transaction->getTime();
        int currentTimestamp = this->par->getcurrtime();

        // Consistency Analysis
        if (transactionReplyCount == 3 && transactionSuccessCount > 1) {
            this->logOperationCoordinator(transaction, true);
            this->deleteTransaction(transactionIterator);
            continue;
        }

        if (transactionReplyCount != 3 && transactionSuccessCount == 2) {
            this->logOperationCoordinator(transaction, true);
            this->deleteTransaction(transactionIterator);
            continue;
        }

        if (transactionReplyCount == 3 && transactionSuccessCount < 2) {
            this->logOperationCoordinator(transaction, false);
            this->deleteTransaction(transactionIterator);
            continue;
        }

        if (transactionReplyCount - transactionSuccessCount == 2) {
            this->logOperationCoordinator(transaction, false);
            this->deleteTransaction(transactionIterator);
            continue;
        }

        if (currentTimestamp - transactionTimestamp > 10) {
            this->logOperationCoordinator(transaction, false);
            this->deleteTransaction(transactionIterator);
            continue;
        }
    }
}

void MP2Node::logOperation(Transaction *t, bool isCoordinator, bool success, int transID) {
    switch (t->msgType) {
        case CREATE: {
            if (success) {
                log->logCreateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
            } else {
                log->logCreateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
            }
            break;
        }

        case READ: {
            if (success) {
                log->logReadSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
            } else {
                log->logReadFail(&memberNode->addr, isCoordinator, transID, t->key);
            }
            break;
        }

        case UPDATE: {
            if (success) {
                log->logUpdateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
            } else {
                log->logUpdateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
            }
            break;
        }

        case DELETE: {
            if (success) {
                log->logDeleteSuccess(&memberNode->addr, isCoordinator, transID, t->key);
            } else {
                log->logDeleteFail(&memberNode->addr, isCoordinator, transID, t->key);
            }
            break;
        }
    }
}

void MP2Node::logOperationCoordinator(Transaction *transaction, bool operationSuccess) {
    if (transaction->msgType == MessageType::CREATE)
        this->logCreate(transaction, true, operationSuccess);

    if (transaction->msgType == MessageType::READ)
        this->logRead(transaction, true, operationSuccess);

    if (transaction->msgType == MessageType::UPDATE)
        this->logUpdate(transaction, true, operationSuccess);

    if (transaction->msgType == MessageType::DELETE)
        this->logDelete(transaction, true, operationSuccess);
}

void MP2Node::logCreate(Transaction *transaction, bool isCoordinator, bool createOperationSuccess) {
    if (createOperationSuccess) {
        this->log->logCreateSuccess(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key,
                                    transaction->value);
        return;
    }

    this->log->logCreateFail(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key,
                             transaction->value);
}

void MP2Node::logRead(Transaction *transaction, bool isCoordinator, bool readOperationSuccess) {
    if (readOperationSuccess) {
        this->log->logReadSuccess(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key,
                                  transaction->value);
        return;
    }

    this->log->logReadFail(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key);
}

void MP2Node::logUpdate(Transaction *transaction, bool isCoordinator, bool updateOperationSuccess) {
    if (updateOperationSuccess) {
        this->log->logUpdateSuccess(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key,
                                    transaction->value);
        return;
    }

    this->log->logUpdateFail(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key,
                             transaction->value);
}

void MP2Node::logDelete(Transaction *transaction, bool isCoordinator, bool deleteOperationSuccess) {
    if (deleteOperationSuccess) {
        this->log->logDeleteSuccess(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key);
        return;
    }

    this->log->logDeleteFail(&this->memberNode->addr, isCoordinator, transaction->getId(), transaction->key);
}

void MP2Node::deleteTransaction(map<int, Transaction *>::iterator transactionIterator) {
    delete transactionIterator->second;
    this->transactionsMap->erase(transactionIterator);
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    /*
     * Implement this
     */
    for (const auto &keyValuePair : this->ht->hashTable) {
        string key = keyValuePair.first;
        string value = keyValuePair.second;

        vector<Node> replicas = this->findNodes(key);

        for (auto &replica : replicas) {
            int transactionId = -1;
            Address fromAddress = this->memberNode->addr;
            Address *toAddress = replica.getAddress();
            MessageType msgType = MessageType::CREATE;

            Message *msg = new Message(transactionId, fromAddress, msgType, key, value);
            string msgData = msg->toString();

            this->emulNet->ENsend(&fromAddress, toAddress, msgData);
        }
    }
}
