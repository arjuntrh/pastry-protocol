# Distributed Message Passing Model Using Pastry Protocol

* Built a P2P overlay network and implemented routing and object location using Pastry protocol.
  * Once a new peer joins the network, the leaf-set and routing tables are created/updated for all the appropriate nodes in the network as per the Pastry protocol.
  * Once the network is created, the application begins routing requests. Each peer transmits ​numRequests r​ equests. Requests are transmitted at the rate of 1 request/second to randomly chosen destination nodes.
  * The application keeps track of the number of hops taken to reach the destination for all the requests. The average number of hops that have to be made to deliver the message is returned by the application.
* Utilized asynchronous Akka.NET actors and achieved object location and message transmission in logarithmic time.


* Largest Network tested:
  * Number of Nodes: 10000
  * Number of requests made by each node: 10


### To Run the Application:
* Open a terminal in the project folder and run the command: \
    &nbsp;&nbsp;&nbsp; ```dotnet fsi --langversion:preview project3.fsx arg1 arg2```
    
    * arg1 is the number of nodes
    * arg2 is the number of requests
    


