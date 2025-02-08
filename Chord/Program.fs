open System
open Akka.FSharp
open System.Threading

type NodeMessage =
    | Create
    | Join of int
    | LookupSuccessor of int
    | ReceiveSuccessor of int
    | Stabilize
    | LookupPredecessor
    | ReceivePredecessor of int
    | Notify of int
    | FixFingerTable
    | FindFingerSuccessor of int * int * int
    | UpdateFingerTable of int * int   
    | BeginQuery
    | QueryMessage
    | FindKeySuccessor of int * int * int
    | KeyFound of int

type SimulatorMessage =
    | IncrementConvergedNode of int * int * int

// Simulates the behavior of nodes in a distributed system and tracks convergence metrics.
let simulator (numberOfNodes: int) (mailbox: Actor<_>) = 
    let mutable totalHopCount = 0
    let mutable totalConvergedNodes = 0
    // Recursive function representing the actor's message processing loop
    let rec loop () = actor {
        // Asynchronously wait for a message from the mailbox
        let! msg = mailbox.Receive ()
        // Match the received message to determine the action
        match msg with
        | IncrementConvergedNode (nodeID, hops, numRequest) ->
            // Print information about the converged node
            printfn "Node with ID: %d successfully converged with a hopCount: %d and the total number of requests: %d" nodeID hops numRequest
            // Update the total hop count and converged nodes count
            totalHopCount <- totalHopCount + hops
            totalConvergedNodes <- totalConvergedNodes + 1
            if(totalConvergedNodes = numberOfNodes) then
                printfn "Total number of hops: %d" totalHopCount
                printfn "Total number of nodes: %d" numberOfNodes
                printfn "Average number of hops: %f" ((float totalHopCount) / float (numRequest * numberOfNodes))
                mailbox.Context.System.Terminate() |> ignore
        
        // Continue processing messages by calling the loop recursively
        return! loop ()
    }

    // Start the message processing loop
    loop ()

// Constructs the Akka.NET actor path for a given actor name.
let getActorPath actorName =
    // Concatenate the actor name with the system and user paths
    let actorPath = @"akka://my-system/user/" + string actorName
    actorPath

// Checks if a value is in between two boundaries in a cyclic hash space, excluding both boundaries.
let isValueInBetweenExcludingBounds hashSpace leftExclusive value rightExclusive =
    // Correct the right boundary to account for cyclic wrapping
    let correctedRight = if(rightExclusive < leftExclusive) then rightExclusive + hashSpace else rightExclusive
    // Correct the value to account for cyclic wrapping and check if it is in the specified range
    let correctedValue = if((value < leftExclusive) && (leftExclusive > rightExclusive)) then (value + hashSpace) else value
    // Return true if the value is equal to the right boundary or in between the corrected boundaries
    (leftExclusive = rightExclusive) || ((correctedValue > leftExclusive) && (correctedValue < correctedRight))

// Checks if a value is in between a left boundary (exclusive) and a right boundary (inclusive) in a cyclic hash space.
let isValueInBetweenExcludingLeftIncludingRight hashSpace leftExclusive value rightInclusive =
    let correctedRight = if(rightInclusive < leftExclusive) then rightInclusive + hashSpace else rightInclusive
    let correctedValue = if((value < leftExclusive) && (leftExclusive > rightInclusive)) then (value + hashSpace) else value
    // Return true if the value is equal to the right boundary or in between the adjusted boundaries
    (leftExclusive = rightInclusive) || ((correctedValue > leftExclusive) && (correctedValue <= correctedRight))

let myActor (nodeID: int) m maxRequests simulatorRef (mailbox: Actor<_>) =
    printfn "[INFO] Creating node %d" nodeID
    let hashSpace = int (Math.Pow(2.0, float m))
    let mutable predecessorID = -1
    let mutable fingerTable = Array.create m -1
    let mutable nextIndex = 0
    let mutable totalHops = 0
    let mutable numberOfRequests = 0

    let rec loop () = actor {
        // Asynchronously wait for a message from the mailbox
        let! msg = mailbox.Receive ()
        // Get the sender's actor reference
        let sender = mailbox.Sender ()

        // Match the received message to determine the action
        match msg with
        | Create ->
            // Initialize the actor state on creation
            predecessorID <- -1
            // Initialize finger table with the current actor's ID
            for i = 0 to m - 1 do
                fingerTable.[i] <- nodeID
            // Schedule stabilization and finger table fix tasks
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                mailbox.Self,
                Stabilize
            )
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    FixFingerTable
                )

        | Join (nDash) ->
            // Reset predecessor as the new node is joining
            predecessorID <- -1
            // Build the actor path for the new node
            let nDashPath = getActorPath nDash
            // Get the actor reference for the new node
            let nDashRef = mailbox.Context.ActorSelection nDashPath
            // Send a request to the new node to find its successor
            nDashRef <! LookupSuccessor (nodeID)

        | LookupSuccessor (id) ->
            // Check if the target ID falls within the immediate successor
            if(isValueInBetweenExcludingLeftIncludingRight hashSpace nodeID id fingerTable.[0]) then
                // If yes, send the successor information to the target node
                let newNodePath = getActorPath id
                let newNodeRef = mailbox.Context.ActorSelection newNodePath
                newNodeRef <! ReceiveSuccessor (fingerTable.[0])
            else
                // If not, iterate through the finger table to find the closest preceding node
                let mutable i = m - 1
                while(i >= 0) do
                    if(isValueInBetweenExcludingBounds hashSpace nodeID fingerTable.[i] id) then
                        // Found the closest preceding node, send a lookup request to it
                        let closestPrecedingNodeID = fingerTable.[i]
                        let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                        let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                        closestPrecedingNodeRef <! LookupSuccessor (id)
                        i <- -1
                    i <- i - 1

        | ReceiveSuccessor (succesorID) ->
            // Update the finger table with the received successor ID   
            for i = 0 to m - 1 do
                fingerTable.[i] <- succesorID
            // Start the scheduler for stabilizing the network
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                mailbox.Self,
                Stabilize
            )
            // Start the scheduler for fixing the finger table
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    FixFingerTable
                )

        | Stabilize ->
            // Get the successor ID from the finger table
            let successorID = fingerTable.[0]
            // Build the actor path for the successor
            let succesorPath = getActorPath successorID
            // Get the actor reference for the successor
            let succesorRef = mailbox.Context.ActorSelection succesorPath
            // Send a request to the successor to lookup its predecessor
            succesorRef <! LookupPredecessor

        | LookupPredecessor ->
            // Send the predecessor ID to the requesting actor
            sender <! ReceivePredecessor (predecessorID)

        | ReceivePredecessor (x) ->
            // Check if the received predecessor ID is within the correct range
            if((x <> -1) && (isValueInBetweenExcludingBounds hashSpace nodeID x fingerTable.[0])) then
                // Update the finger table with the new predecessor ID
                fingerTable.[0] <- x
            // Get the successor ID from the updated finger table
            let successorID = fingerTable.[0]
            // Build the actor path for the successor
            let succesorPath = getActorPath successorID
            // Get the actor reference for the successor
            let successorRef = mailbox.Context.ActorSelection succesorPath
            // Notify the successor about the potential new predecessor
            successorRef <! Notify (nodeID)

        | Notify (nDash) ->
            // Check if the current predecessor is not set or the new predecessor is within the correct range
            if((predecessorID = -1) || (isValueInBetweenExcludingBounds hashSpace predecessorID nDash nodeID)) then
                // Update the predecessor with the new value
                predecessorID <- nDash

        | FixFingerTable ->
            // Increment the index for fixing the finger table
            nextIndex <- nextIndex + 1
            // Reset the index to the beginning if it exceeds the finger table size
            if(nextIndex >= m) then
                nextIndex <- 0
            // Calculate the next finger value using the formula
            let fingerValue = nodeID + int (Math.Pow(2.0, float (nextIndex)))
            // Send a request to find the successor for the calculated finger value
            mailbox.Self <! FindFingerSuccessor (nodeID, nextIndex, fingerValue)

        | FindFingerSuccessor (originNodeID, nextIndex, id) ->
            // Check if the ID falls within the immediate successor
            if(isValueInBetweenExcludingLeftIncludingRight hashSpace nodeID id fingerTable.[0]) then
                // If yes, update the finger table for the origin node
                let originNodePath = getActorPath originNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! UpdateFingerTable (nextIndex, fingerTable.[0])
            else
                // If not, iterate through the finger table to find the closest preceding node
                let mutable i = m - 1
                while(i >= 0) do
                    if(isValueInBetweenExcludingBounds hashSpace nodeID fingerTable.[i] id) then
                        // Found the closest preceding node, send a request to it to find the finger successor
                        let closestPrecedingNodeID = fingerTable.[i]
                        let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                        let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                        closestPrecedingNodeRef <! FindFingerSuccessor (originNodeID, nextIndex, id)
                        i <- -1
                    i <- i - 1

        | UpdateFingerTable (nextIndex, fingerSuccessor) ->
            // Update the finger table with the new successor for the specified finger index
            fingerTable.[nextIndex] <- fingerSuccessor

        | BeginQuery ->
        // Check if the node can still perform queries
            if(numberOfRequests < maxRequests) then
                // Initiate a query and schedule the next query after a delay
                mailbox.Self <! QueryMessage
                mailbox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), mailbox.Self, BeginQuery)
            else
                // Node has completed querying. Send its current status to the hop counter
                simulatorRef <! IncrementConvergedNode (nodeID, totalHops, numberOfRequests)

        | QueryMessage ->
            // Generate a random key for the query
            let key = (System.Random()).Next(hashSpace)
            // Initiate the process to find the successor for the generated key
            mailbox.Self <! FindKeySuccessor (nodeID, key, 0)

        // Scalable key lookup
        | FindKeySuccessor (originNodeID, id, numHops) ->
            // Check if the current node contains the target key
            if(id = nodeID) then
                // Notify the requesting node that the key is found
                let originNodePath = getActorPath originNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! KeyFound (numHops)
            // Check if the target key falls within the immediate successor
            elif(isValueInBetweenExcludingLeftIncludingRight hashSpace nodeID id fingerTable.[0]) then
                // Notify the requesting node that the key is found at the immediate successor
                let originNodePath = getActorPath originNodeID
                let originNodeRef = mailbox.Context.ActorSelection originNodePath
                originNodeRef <! KeyFound (numHops)
            else
                // Iterate through the finger table to find the closest preceding node
                let mutable i = m - 1
                while(i >= 0) do
                    if(isValueInBetweenExcludingBounds hashSpace nodeID fingerTable.[i] id) then
                        // Found the closest preceding node, send a request to it to find the key successor
                        let closestPrecedingNodeID = fingerTable.[i]
                        let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                        let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                        closestPrecedingNodeRef <! FindKeySuccessor (originNodeID, id, numHops + 1)
                        i <- -1
                    i <- i - 1

        | KeyFound (hopCount) ->
            // Check if the node can still perform queries
            if(numberOfRequests < maxRequests) then
                // Update the total hops and increment the number of completed queries
                totalHops <- totalHops + hopCount
                numberOfRequests <- numberOfRequests + 1

        return! loop ()
    }
    loop ()

[<EntryPoint>]
let main argv =
    
    // Create the system
    let systemRef = System.create "my-system" (Configuration.load())

   // Parse command line arguments for the number of nodes and requests
    let numNodes = int argv.[0]
    let numRequests = int argv.[1]

    // Define the length of the m-bit identifier and calculate the hash space size
    let m = 20
    let hashSpace = int (Math.Pow(2.0, float m))

    // Spawn the simulator actor to coordinate the simulation
    let simulatorRef = spawn systemRef "simulator" (simulator(numNodes))

    // Initialize arrays to store node IDs and references
    let nodeIDs = Array.create numNodes -1;
    let nodeRefs = Array.create numNodes null;
    let mutable index = 0;

    // Create nodes with random IDs and spawn corresponding actors
    while index < numNodes do
        try
            let nodeID  = (Random()).Next(hashSpace)
            nodeIDs.[index] <- nodeID
            nodeRefs.[index] <- spawn systemRef (string nodeID) (myActor nodeID m numRequests simulatorRef)
            
            // create the first node and join others to it
            if index = 0 then
                nodeRefs.[index] <! Create
            else
                nodeRefs.[index] <! Join(nodeIDs.[0])
            index <- index + 1
            Thread.Sleep(500)
        with _ -> ()

    // Wait for some time to get system stabilized
    printfn "30 sec Wait time for system stabilization"
    Thread.Sleep(30000)

    // Initiate querying for each node in the system
    for nodeRef in nodeRefs do
        nodeRef <! BeginQuery
        Thread.Sleep(500)

    // Wait until all actors in the system are terminated
    systemRef.WhenTerminated.Wait()

    0 // return an integer exit code