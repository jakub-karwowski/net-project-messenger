# Tcp p2p messaging app

## cross thread communication

- Keyboard -render q-> Render
- Render -store q-> Store
- Store -render q-> Render
- Listener -store q-> Store
- Store -router q-> Router
- Router -pt-i q-> Pt-i
- Pt-i -router q-> Router
- Pt-i -balance q-> Balancer (heart beat msg route)
- Router -balance q-> Balancer
- Balancer -router q-> Router

## other threads

- Logger (logger queue, all threads)
- Balancer recives logglike msgs, computes load and makes decisions; also monitors heartbeat from PT-i

## data structures

- Store: mapping PeerTable[(my_id, peer_id, conv_id)]; ConvStore
- Router: mapping ConnTable[(ptr TCPconn, pt_id, conv_id, connection_status)]; assignment_policy[] (pt-i number to assign kth arriving connection, produced by balancer)
- Balancer: mapping LoadLog[(pt_id, last_read, bytes_in, bytes_out, per_covn_id[])] (store since last check); assignment_policy
- Pt-i: [ConvHandle(ptr TCPconn, conv_id: pool_id (mapping))]

## scenarios

### Send Msg

1. Render -> Store [Store: update(msg_id, pending_send)]
2. Store -> Router [Router: find owning pt-i]
3. Router -> Pt-i [Pt-i: send unblocking]
4. Pt-i -> Router 
5. Router -> Store [Store: update(msg_id, sent)]
6. Router -> Balancer [Balancer: update LoadLog]
7. Store -> Render [Store: emit msg_send on subscribed]

### Recieve Msg

1. Pt-i -> Router
2. Router -> Store [Store: update(new_msg_id(), received)]
2. Router -> Balancer [Balancer: update LoadLog]
3. Store -> Render [Store: emit recieved_msg on subscribed]

### New incoming connection

1. Listener -> Store [Listener: accept; Store: check if peer_id exists in PeerTable and retrive/insert update(conv_id, pending_conn)]
2. Store -> Router -> Pt-i [Router: according to a policy created by Balancer append to exisiting Pt-i or create new; Pt-i: append to epoll]
3. Pt-i -> Router [Pt-i mark connected]
4. Router -> Balancer
5. Router -> Store [Store: update(conv_id, connected)]
6. Store -> Render [Store: emit connected on subscribed]

### Dial

1. Render -> Store [Store: update(new_conv_id()/conv_id, pending_dial]
2. Store -> Router [Router: under mutex on LoadLog: choose Pt-i or create new, create Tcp conn pass to Pt-i]
3. Router -> Pt-i [Pt-i: Tcp connect to peer (do not block)]
4. Pt-i -> Router []
4. Router -> Balancer [Balancer: update LoadLog; Pt-i mark connected] or failed
5. Router -> Store [Store: update(conv_id, connected)] or failed
6. Store -> Render [Store: emit connected on subscribed]

### Peer Disconnection

1. Pr-i -> Router [Pt-i mark disconnected]
1. Router -> Store [Store update(conv_id, disconnecting)]
2. Router -> Balancer [Balancer: update LoadLog]

### Balancer Initiated Disconnect

1. Balancer -> Router [Balancer: send disconnect request; Router: mark disconnecting]
2. Router -> Pt-i [Pt-i: disconnect]
3. Pt-i -> Router [notify Pt-i tcpconnection released]
4. Router -> Balancer [Router: release Tcpconnection; Router: mark disconnected]

### Rebalance

1. Balancer -> Router [Balancer: send move request; Router: mark moving, send TcpConnection relase request]
2. Router -> Pt-i [Pt-i: relases TcpConnection]
3. Pt-i -> Router [Pt-i: reports]
4. Router -> Pt-j [reassigned connection]
5. Pt-j -> Router [reports]
6. Router -> Balancer [updated balancer]

### App closing

1. Render -> Store
2. Store -> Router
3. Router -> Pt-i [Pt-i: close connections, try to send buffered data with a strict time limit]
4. Router -> Balancer [stop balancer]
4. Pt-i -> Router [report closed]
5. Router -> Store
6. Store -> Render
