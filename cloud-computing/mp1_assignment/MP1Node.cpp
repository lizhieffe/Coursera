/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        
        memberNode->inGroup = true;
        
        free(msg);
    }

    return 1;

}

void MP1Node::sendHeartBeating(vector<Address> addresses) {
    
    MessageHdr *msg;
    
    int localId = getLocalId();
    short localPort = getLocalPort();
    
    /*
     * iterate the addresses to send message
     */
    for (Address addr : addresses) {
        
        // don't send to myself
        if (*(int*)addr.addr == getLocalId())
            continue;
        
        bool toNextAddr = false;
        
        // don't send to inactive node
        for (int j = 0; j < deadMembers.size(); ++j)
            if (deadMembers[j] == *(int*)addr.addr) {
                toNextAddr = true;
                break;
            }
        if (toNextAddr == true)
            continue;
        
        int msgsize = sizeof(MessageHdr) + sizeof(addr.addr) + sizeof(long) + 1;
        
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = HEART_BEATING_REQ;
        
        int id;
        short port;
        long heartbeat;
        
        // send heart beat to other member
        for (int i = (int)memberNode->memberList.size(); i >= 0; --i) {
            if (i == memberNode->memberList.size()) {
                id = localId;
                port = localPort;
                heartbeat = memberNode->heartbeat;
            }
            else {
                id = memberNode->memberList[i].id;
                port = memberNode->memberList[i].port;
                heartbeat = memberNode->memberList[i].heartbeat;
            }
            
            // don't redirect message of inactive node
            bool toNextId = false;
            for (int j = 0; j < deadMembers.size(); ++j)
                if (deadMembers[j] == id) {
                    toNextId = true;
                    break;
                }
            if (toNextId == true)
                continue;
            
            memcpy((char *)(msg+1), &id, sizeof(int));
            memcpy((char *)(msg+1) + 0 + 4, &port, sizeof(short));
            memcpy((char *)(msg+1) + 0 + sizeof(memberNode->addr.addr), &heartbeat, sizeof(long));
            
            // send HEART_BEATING message to introducer member
            emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
            
            cout << "Node[" << localId << "]->Node[" << *(int*)addr.addr << "] Local Heartbeat[" << getLocalHeartbeat() << "] sent hearbeat " << ": id = " << id << ", port = " << port << ", heartbeat = " << heartbeat << endl;
        }
        
        free(msg);
    }
    memberNode->heartbeat = memberNode->heartbeat + 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    // DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
    //              the nodes
    //              Propagate your membership list
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    
    int localId = getLocalId();
    
    /*
     * update the member list if the message is hearbeat
     */
    if (((MessageHdr *)data)->msgType == HEART_BEATING_REQ) {
        int id = *(int *)((char *)((MessageHdr *)data + 1) + 0);
        short port = *(short *)((char *)((MessageHdr *)data + 1) + 4);
        long heartbeat = *(long *)((char *)((MessageHdr *)data + 1) + 6);
        long currTime = time(nullptr);
        
        /*
         * don't record the self
         */
        if (id == localId)
            return false;
        
        cout << "Node[" << localId << "] get heartbeat from <- id = " << id << ", port = " << port << ", heartbeat = " << heartbeat << endl;
        
        for (vector<MemberListEntry>::size_type i = 0; i < memberNode->memberList.size(); ++i) {
            if (memberNode->memberList[i].id == id) {
                if (memberNode->memberList[i].heartbeat < heartbeat) {
                    memberNode->memberList[i].timestamp = currTime;
                    memberNode->memberList[i].heartbeat = heartbeat;
                    return true;
                }
                else {
                    return false;
                }
            }
        }
        MemberListEntry newEntry(id, port, heartbeat, currTime);
        memberNode->memberList.push_back(newEntry);
        
        string addedAddr;
        addedAddr.append(to_string(id));
        addedAddr.append(":");
        addedAddr.append(to_string(port));
        Address addedAddrObj(addedAddr);
        log->logNodeAdd(&memberNode->addr, &addedAddrObj);
        
        return true;
    }
    
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    long MEMBERSHIP_TIMEOUT = 40;
    
    for (vector<MemberListEntry>::size_type i = 0; i < memberNode->memberList.size(); ++i) {
        cout << getLocalHeartbeat() << " : " <<memberNode->memberList[i].heartbeat << endl;
        if (getLocalHeartbeat() - memberNode->memberList[i].heartbeat > MEMBERSHIP_TIMEOUT) {
            bool hasBeenRemoved = false;
            int id = memberNode->memberList[i].id;
            for (vector<int>::size_type j = 0; j < deadMembers.size(); ++j) {
                if (deadMembers[j] == id) {
                    hasBeenRemoved = true;
                    break;
                }
            }
            if (hasBeenRemoved == false) {
                string removedAddr;
                removedAddr.append(to_string(id));
                removedAddr.append(":");
                removedAddr.append(to_string(memberNode->memberList[i].port));
                Address removedAddrObj(removedAddr);

                log->logNodeRemove(&memberNode->addr, &removedAddrObj);
                deadMembers.push_back(id);
            }
        }
        
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

int MP1Node::getLocalId() {
    return *(int *)(memberNode->addr.addr);
}

short MP1Node::getLocalPort() {
    return *(short *)(&memberNode->addr.addr[4]);

}

long MP1Node::getLocalHeartbeat() {
    return memberNode->heartbeat;
}