import threading
import socket
import time
import random
import json

# States
FOLLOWER = 'follower'
CANDIDATE = 'candidate'
LEADER = 'leader'

class Node(threading.Thread):
    def __init__(self, node_id, all_nodes, base_port=5000):
        super().__init__()
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.term = 0
        self.voted_for = None
        self.state = FOLLOWER
        self.leader_id = None

        # Election and heartbeat settings
        self.election_timeout = self.reset_election_timeout()
        self.heartbeat_interval = 1.0  # leader heartbeat frequency

        # UDP socket configuration
        self.port = base_port + node_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", self.port))
        self.sock.settimeout(0.5)  # Non-blocking receive with timeout

        # State for election
        self.votes_received = 0

        # Stop event
        self.stop_event = threading.Event()

    def reset_election_timeout(self):
        return time.time() + random.uniform(2.0, 4.0)

    def send_message(self, target_id, msg_type, data=None):
        if data is None:
            data = {}
        message = {
            'type': msg_type,
            'term': self.term,
            'from': self.node_id,
            'data': data
        }
        msg_bytes = json.dumps(message).encode('utf-8')
        target_port = 5000 + target_id
        self.sock.sendto(msg_bytes, ("127.0.0.1", target_port))

    def broadcast_message(self, msg_type, data=None):
        for n in self.all_nodes:
            if n != self.node_id:
                self.send_message(n, msg_type, data)

    def become_follower(self, term, leader_id=None):
        self.state = FOLLOWER
        self.term = term
        self.leader_id = leader_id
        self.voted_for = None
        self.votes_received = 0
        self.election_timeout = self.reset_election_timeout()
        print(f"Node {self.node_id} -> Follower (term {self.term}, leader {leader_id})")

    def become_candidate(self):
        self.state = CANDIDATE
        self.term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        self.election_timeout = self.reset_election_timeout()
        print(f"Node {self.node_id} -> Candidate (term {self.term})")
        # Send RequestVote to all other nodes
        self.broadcast_message('RequestVote', {'last_log_index': 0, 'last_log_term': 0})

    def become_leader(self):
        self.state = LEADER
        self.leader_id = self.node_id
        print(f"Node {self.node_id} -> Leader (term {self.term})")
        # Immediately send heartbeat
        self.broadcast_message('Heartbeat', {})

    def handle_message(self, msg):
        msg_type = msg['type']
        msg_term = msg['term']
        msg_from = msg['from']
        data = msg.get('data', {})

        if msg_term > self.term:
            # If we receive a message with a higher term, revert to follower
            self.become_follower(msg_term)

        if msg_type == 'RequestVote':
            # Candidate is requesting a vote
            vote_granted = False
            if (self.voted_for is None or self.voted_for == msg_from) and msg_term >= self.term:
                self.voted_for = msg_from
                vote_granted = True
            # Reply with a vote
            self.send_message(msg_from, 'VoteResponse', {'vote_granted': vote_granted})

        elif msg_type == 'VoteResponse' and self.state == CANDIDATE:
            if data.get('vote_granted'):
                self.votes_received += 1
                # Check if we have majority
                if self.votes_received > len(self.all_nodes) // 2:
                    self.become_leader()

        elif msg_type == 'Heartbeat':
            # Heartbeat from the leader
            if msg_term >= self.term:
                # Reset election timeout
                if self.state != FOLLOWER:
                    self.become_follower(msg_term, msg_from)
                else:
                    self.leader_id = msg_from
                    self.election_timeout = self.reset_election_timeout()

    def run(self):
        while not self.stop_event.is_set():
            # Leader sending periodic heartbeats
            if self.state == LEADER:
                print(f"Node {self.node_id} sending heartbeat")
                self.broadcast_message('Heartbeat', {})
                time.sleep(self.heartbeat_interval)
                continue

            # Check if election timeout has passed (for Follower or Candidate)
            if self.state in [FOLLOWER, CANDIDATE]:
                if time.time() > self.election_timeout:
                    # Start a new election
                    self.become_candidate()

            if self.stop_event.is_set():
                break

            try:
                data, addr = self.sock.recvfrom(4096)
                msg = json.loads(data.decode('utf-8'))
                self.handle_message(msg)
            except socket.timeout:
                # No message received
                pass

            time.sleep(0.1)  # Slight delay to reduce busy-waiting

        # Clean up
        self.sock.close()

    def stop(self):
        self.stop_event.set()


if __name__ == "__main__":
    num_nodes = 5
    nodes = list(range(num_nodes))
    node_threads = []

    # Start all nodes
    for i in nodes:
        node = Node(i, nodes, base_port=5000)
        node.start()
        node_threads.append(node)

    # Let the simulation run for some time
    try:
        time.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        # Stop all nodes
        for n in node_threads:
            n.stop()
        for n in node_threads:
            n.join()
