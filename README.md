Newton
======

Newton is an RPC server which's built upon HTTP/2 protocol. It also acts like a service discovery server for its clients. Anyone who wants to build
a system which follows microservice pattern, may found Newton very useful.

Installation
------------

You need to install and configure [Go](https://golang.org/doc/install) before installing Newton. 

Clone the repository:

```
git clone https://github.com/purak/newton.git
```

Get the vendored dependencies:

```
git submodule update --init --recursive
git submodule foreach --recursive git clean -df
git submodule foreach --recursive git reset --hard

```

Then, install it:

```
go install -v
```

Now, you should run the app by calling newton. It should be installed to your GOPATH.


You can find a sample configuration file with name newton.yml in the code base. Default configuration file path is **/etc/newton.yml**. If you want to modify this path, 
you can do it by setting **NEWTON_CONFIG** environment variable before running Newton. 

If you want to learn more about Newton's configuration, please take look at Configuration page at the project wiki.

Endpoints
-----------

* /events/:name
* /lookup/:name	
* /read/:name/:clientID/:routingKey
* /write-with-id/:messageID
* /write/:name/:clientID/:routingKey
* /read-with-id/:messageID
* /close/:messageID/:returnCode


Basic Design and Terminology
----------------------------

Newton has been built upon HTTP. So you can create RPC systems with Newton using HTTP's well-known request-response
cycle. You don't need to integrate a different protocol(AMQP or ZeroMQ) into your application and create complex structures 
to manage that integration. You only need a proper HTTP library to use.

I hardly recommend to use Newton with HTTP/2. Please take look at *Why HTTP/2* part of this document.

Newton can also be used as a service discovery system. Every Newton node runs its own distributed hash table instance to find 
and keep information about the clients of cluster. It's a modified implementation of Kademlia algorithm of the BitTorrent world.

Every Newton client provides its own name to connect to the cluster. It can be an ordinary string(e.g. foobar) or a SHA-1 
value of something(e.g. fingerprint of your PGP key). SHA-1 value of the string or directly SHA-1 you provided is called 
as **UserHash**. After creating connection, the server returns a number to you. That number is your **ClientID**. 


UserHash, ClientID and NodeID of the Newton server you connected are identical to your client in the cluster. Other clients 
can discover your client if they know your UserHash.  

Why DHT?
========
Newton is designed as a Internet-wide RPC system to create a free network of communication. In order to achieve that goal, I need to find a way without any
SPOF mechanisim. So Paxos, RAFT or other consensus algorithms could not be used in Newton for synchronization. 

Basically I needed an algorithm to find a resource in a very large overlay on the Internet. As you may know, there is a good sample of this: torrenting! 
In BitTorrent world, you have info hashes. Every torrent has its unique info hash and with this unique key, you can discover other computers in the torrent swarm.

Newton uses the same paradigm. Every client of a Newton cluster has its own UserHash and they discover each other using their UserHashes. 

As I mentioned before, Newton uses a slightly modified version of original Kademlia algorithm to distribute and discover availability information of its clients.

For more information, you can dive into the code or read some papers on Kademlia and DHT protocol of BitTorrent.

* [BEP-5](http://www.bittorrent.org/beps/bep_0005.html)
* [Original Kademlia Paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)

Why HTTP/2?
===========
With HTTP/2, every Newton client uses only one TCP socket to communicate with its server. That's one of the main tricks with Newton.

Sample Application
==================
I have built a static file server as a Newton client. Actually, it's not an HTTP server. If you know something serious about HTTP protocol, you can grasp the logic behind the app without any problem. 
So anyone wants to run this on his computer, just need to run it with proper parameters. No available port or public IP address are required to serve files to outside with this tool. People who want to
list or download files from your local computer, just need to run a lookup query on the cluster with your name/UserHash.

```
curl http://demo.newton.network:46523/lookup/purak
```

This should return a JSON like this:

```
{"result":{"https://demo.newton.network:46523":[4]}}
```

The above thing means that my client has a session on https://demo.newton.network:46523 with ClientID 4. You can access its file listing page by visiting [https://demo.newton.network:46523/read/purak/4/](https://demo.newton.network:46523/read/purak/4/)
You should expect to see an ordinary index page. It's just like index page of nginx. 


[LINK]

Contributions
-------------

Newton is still an **alpha** software and needs too many interest to work fine. So your patches are welcome. Please
don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

License
-------

The Apache License, Version 2.0 - see LICENSE for more details.
