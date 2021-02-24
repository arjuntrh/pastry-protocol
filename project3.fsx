#time "on"
#r "nuget: Akka.FSharp" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Diagnostics
open Akka.Configuration

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            loglevel: ERROR
        }")

let system = System.create "system" <| configuration
let mutable mainRef = Unchecked.defaultof<_>

let timer = Stopwatch()

// Initialization
let mutable globalNumNodes = 0
let mutable globalNumRequests = 0
let mutable hashLength = 0
let actorsNodeIDList = new List<string>()
let nodeIDToActorRefMap = new Dictionary<string, IActorRef>()

//Configuration Parameters
let b = 4
let leafSetLimit = (Math.Pow(2.0, b|>float)) |> int
let columns = (Math.Pow(2.0, b|>float)) |> int


type Num = int

type ChildActorMessage =
    |SetupNode of string
    |JoinNetwork of string * int
    |UpdateRoutingTable of string[]
    |Routing of string * string * int
    

type ParentActorMessage = 
    |ProcessInput of int * int
    |RoutingTableCreated of int
    |InitiateRouting
    |MessageReachedDestination of int

let hexToDecimal(hexValue:string) = 
    Convert.ToInt32(hexValue , 16)

let findHexValueDifference (hexValue1:string, hexValue2:string) = 
    let intValue1 = Convert.ToInt32(hexValue1 , 16)
    let intValue2 = Convert.ToInt32(hexValue2 , 16)
    Math.Abs(intValue1 - intValue2)

let findPrefixMatching(nodeID1:string, nodeID2:string) =
    let mutable prefixMatchLength = 0
    let mutable matching = true
    while (prefixMatchLength < nodeID1.Length && matching) do
        if(nodeID1.[prefixMatchLength] = nodeID2.[prefixMatchLength]) then
            matching <- true
            prefixMatchLength <- prefixMatchLength + 1
        else
            matching <- false
    
    prefixMatchLength

let convertToNodeIDFormat (id:int) =
    let hexValue = id.ToString("X")
    let hash = "0"
    let nodeID = String.replicate (hashLength - hexValue.Length) hash + hexValue
    nodeID

let clone i (arr:'T[,]) = arr.[i..i, *]|> Seq.cast<'T> |> Seq.toArray
let mutable parentActorRefStore = Unchecked.defaultof<_>

let childActor (mailbox: Actor<_>) =
    let mutable nodeID = ""
    let mutable routingTable: string[,] = Array2D.zeroCreate hashLength columns
    let leafSet = new HashSet<string>()
    let mutable currentRow = 0
   
    let rec childActorLoop () = actor {

        let! message = mailbox.Receive()
        let childActorSender = mailbox.Sender()

        match message with

        | SetupNode (incomingNodeID) ->
            parentActorRefStore <- childActorSender
            nodeID <- incomingNodeID
            
            let nodeIDDecimal = hexToDecimal(nodeID)
            let mutable smallerLeaf = nodeIDDecimal
            let mutable biggerLeaf = nodeIDDecimal

            for i = 1 to leafSetLimit do
                if i <= 8 then
                    if smallerLeaf = 0 then
                        smallerLeaf <- nodeIDToActorRefMap.Count-1
                    else
                        smallerLeaf <- smallerLeaf - 1

                    leafSet.Add(convertToNodeIDFormat(smallerLeaf)) |> ignore                
                else
                    if biggerLeaf = nodeIDToActorRefMap.Count-1 then
                        biggerLeaf <- 0
                    else
                        biggerLeaf <- biggerLeaf + 1

                    leafSet.Add(convertToNodeIDFormat(biggerLeaf)) |> ignore

        | JoinNetwork(incomingNodeID, currentIndex) ->
            let mutable i = 0
            let mutable k = currentIndex

            while i < incomingNodeID.Length && incomingNodeID.[i] = nodeID.[i] do
                i<- i+1

            let commonPrefixLength = i
            let mutable currentRow : string[] = Array.zeroCreate 0

            while k <= commonPrefixLength do
                currentRow <- clone k routingTable             
                currentRow.[Int32.Parse(nodeID.[commonPrefixLength].ToString(), Globalization.NumberStyles.HexNumber)] <- nodeID

                if (nodeIDToActorRefMap.ContainsKey(incomingNodeID)) then
                    let incomingNodeIDRef = nodeIDToActorRefMap.[incomingNodeID]
                    incomingNodeIDRef <! UpdateRoutingTable(currentRow)
                else
                    printfn "NodeID does not exist in the map"

                k <- k+1

            let targetRow = commonPrefixLength
            let targetColumn = Int32.Parse(incomingNodeID.[commonPrefixLength].ToString(), Globalization.NumberStyles.HexNumber)
             
            if isNull routingTable.[targetRow, targetColumn] then
                routingTable.[targetRow, targetColumn] <- incomingNodeID
                parentActorRefStore <! RoutingTableCreated (1)
            else
                let nearestNodeID = routingTable.[targetRow, targetColumn]
                if (nodeIDToActorRefMap.ContainsKey(nearestNodeID)) then
                    let nearestNodeIDRef = nodeIDToActorRefMap.[nearestNodeID]
                    nearestNodeIDRef <! JoinNetwork(incomingNodeID, k)
                else
                    printfn "nearestNeighborID does not exist in the map"

        | UpdateRoutingTable(row: String[])->          
            routingTable.[currentRow, *] <- row
            currentRow <- currentRow + 1

        | Routing (destinationNodeID, sourceNodeID, hops) ->
            let mutable nextPossibleNode = ""

            // check if it is in the leaf set

            if leafSet.Contains(destinationNodeID) then
                nextPossibleNode <- destinationNodeID
            else
                let prefixValue = findPrefixMatching(nodeID, destinationNodeID)
                let nextRow =  prefixValue
                let nextColumn = hexToDecimal(destinationNodeID.[prefixValue].ToString())
                nextPossibleNode <- routingTable.[nextRow, nextColumn]

                if isNull nextPossibleNode then
                    nextPossibleNode <- routingTable.[nextRow, 0]

            // printf "nextPossibleNode = %s" nextPossibleNode
            if(nextPossibleNode = destinationNodeID) then
                parentActorRefStore <! MessageReachedDestination (hops + 1)
            else
                let nextActorReference = nodeIDToActorRefMap.[nextPossibleNode]
                nextActorReference <! Routing (destinationNodeID, sourceNodeID, hops + 1)
            
        |_ -> printfn ""


        return! childActorLoop ()
    }
    childActorLoop ()

let computeHashLength () = 
    hashLength <- Math.Log(globalNumNodes |> float, 16.0) |> ceil |> int


let createNodeID (actorRef:IActorRef, id:int) =
    let hexValue = id.ToString("X")
    let hash = "0"
    let nodeID = String.replicate (hashLength - hexValue.Length) hash + hexValue
    nodeIDToActorRefMap.Add(nodeID, actorRef)
    actorsNodeIDList.Add(nodeID)
    nodeID

let rec generateHashForActor (actorRef:IActorRef) = 
    let possibleHashValues = ['0';'1';'2';'3';'4';'5';'6';'7';'8';'9';'A';'B';'C';'D';'E';'F']
    let findRandomHashUnit = Random()

    let mutable hashValue:string = ""
    for i = 1 to hashLength do
        let randomIndex = findRandomHashUnit.Next(0, 16)
        hashValue <- hashValue + (possibleHashValues.[randomIndex] |> string)

    if nodeIDToActorRefMap.ContainsKey(hashValue) then
        generateHashForActor(actorRef)
    else
        nodeIDToActorRefMap.Add(hashValue, actorRef)
        hashValue


let parentActor (mailbox: Actor<_>) =
    let mutable routingTableCreatedCount = 0
    let childActorsList = new List<IActorRef>()
    let mutable messagesReachedDestination = 0.0
    let mutable totalHops = 0
    let mutable expectedOutputCount = 0.0

    let rec parentActorLoop () = actor {
        
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | ProcessInput(numNodes, numRequests) ->
            mainRef <- sender
            globalNumNodes <- numNodes
            globalNumRequests <- numRequests

            //Compute output count and Node Hash Length
            expectedOutputCount <- (globalNumNodes * globalNumRequests) |> float
            computeHashLength()

            // 1.Create an initial actor/node
            let initialNode = spawn system (string(0)) childActor
            let nodeID = createNodeID(initialNode, 0)
            initialNode <! SetupNode(nodeID)

            //2. Create and initiliaze other actors/nodes
            for i = 1 to numNodes-1 do
               let workerRef = spawn system (string(i)) childActor
               let nodeID = createNodeID(workerRef, i)
               workerRef <! SetupNode(nodeID)
               initialNode <! JoinNetwork(nodeID, 0)

        | RoutingTableCreated(actorNodeID) ->
            routingTableCreatedCount <- routingTableCreatedCount + 1
            if(routingTableCreatedCount = globalNumNodes - 1) then
                printfn "Network built..."

                Async.Sleep(1000) |> Async.RunSynchronously
                
                printfn "Begin routing..."

                mailbox.Self <! InitiateRouting

        | InitiateRouting ->
            timer.Start()
            for i = 1 to globalNumRequests do
                for nodeID in actorsNodeIDList do
                    
                    let rec findRandomNode () = 
                        let findRandomNodeID = Random()
                        let randomNodeIDIndex = findRandomNodeID.Next actorsNodeIDList.Count
                        let randomDestinationNode = actorsNodeIDList.[randomNodeIDIndex]

                        if randomDestinationNode <> nodeID then
                            let nodeIDRef = nodeIDToActorRefMap.[nodeID]
                            nodeIDRef <! Routing(randomDestinationNode, nodeID, 0)
                        else
                            findRandomNode()
                    
                    findRandomNode ()
                Async.Sleep(1000) |> Async.RunSynchronously
  
        | MessageReachedDestination (hops) ->
            messagesReachedDestination <- messagesReachedDestination + 1.0
            totalHops <- totalHops + hops

            if(messagesReachedDestination = expectedOutputCount) then
                let averageHops = (totalHops |> float) / messagesReachedDestination
                printfn "All requests completed..."
                printfn "Average number of Hops = %f" averageHops
                mainRef <! "Messages transmitted by all the actors successfully"

        |_ -> printfn ""

        return! parentActorLoop()
    }

    parentActorLoop ()

let main() =
    try
        (* Command Line input - num of nodes and num of requests *)
        let mutable numNodes = fsi.CommandLineArgs.[1] |> int
        let numRequests = fsi.CommandLineArgs.[2] |> int

        let parentActorRef = spawn system "parentActor" parentActor
        let parentTask = (parentActorRef <? ProcessInput(numNodes, numRequests))
        let response = Async.RunSynchronously(parentTask)

        // printfn "Convergence Time = %d ms" timer.ElapsedMilliseconds
        printfn "Done"

    with :? TimeoutException ->
        printfn "Timeout!"

main()
system.Terminate()