package main

import (
    "fmt"
    "os"
    "bufio"
    "strings"
    "strconv"
    "hash/fnv"
)

type Peer struct {
    ID         int
    IP         string
    Port       int
    Successor  *Peer
    Predecessor *Peer
    Files      map[string]string // Map of filename to file-ID
}

func hash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32())
}

func NewPeer(ip string, port int) *Peer {
    peerID := hash(fmt.Sprintf("%s:%d", ip, port))
    return &Peer{
        ID:         peerID,
        IP:         ip,
        Port:       port,
        Successor:  nil,
        Predecessor: nil,
        Files:      make(map[string]string),
    }
}

func (p *Peer) joinRing(successor *Peer) {
    p.Successor = successor
    p.Predecessor = successor.Predecessor
    successor.Predecessor.Successor = p
    successor.Predecessor = p
    // Move files from successor whose successor is p
    for filename, fileID := range successor.Files {
        if hash(filename) > p.ID && hash(filename) <= successor.ID {
            p.Files[filename] = fileID
            delete(successor.Files, filename)
        }
    }
}

func (p *Peer) leaveRing() {
    p.Successor.Predecessor = p.Predecessor
    p.Predecessor.Successor = p.Successor
    // Transfer all files to successor
    for filename, fileID := range p.Files {
        p.Successor.Files[filename] = fileID
    }
}

func (p *Peer) storeFile(filename string) {
    fileID := hash(filename)
    p.Successor.Files[filename] = strconv.Itoa(fileID)
}

func (p *Peer) retrieveFile(filename string) (string, bool) {
    fileID := hash(filename)
    if _, exists := p.Successor.Files[filename]; exists {
        return filename, true
    }
    return "", false
}

func main() {
    if len(os.Args) != 2 {
        fmt.Println("Usage: ./task2-peer <port>")
        return
    }

    port, err := strconv.Atoi(os.Args[1])
    if err != nil {
        fmt.Println("Invalid port number")
        return
    }

    peer := NewPeer("localhost", port)

    fmt.Println("Peer started on port", port)

    reader := bufio.NewReader(os.Stdin)

    for {
        fmt.Println("\nPlease select an option:")
        fmt.Println("1) Enter the peer address to connect")
        fmt.Println("2) Enter the key to find its successor")
        fmt.Println("3) Enter the filename to take its hash")
        fmt.Println("4) Display my-id, succ-id, and pred-id")
        fmt.Println("5) Display the stored filenames and their keys")
        fmt.Println("6) Exit")

        fmt.Print("> ")
        optionStr, _ := reader.ReadString('\n')
        optionStr = strings.TrimSpace(optionStr)
        option, err := strconv.Atoi(optionStr)
        if err != nil {
            fmt.Println("Invalid option")
            continue
        }

        switch option {
        case 1:
            fmt.Print("Enter the peer address to connect: ")
            peerAddress, _ := reader.ReadString('\n')
            peerAddress = strings.TrimSpace(peerAddress)
            // Connect to peerAddress
            fmt.Println("Connection Established")
        case 2:
            fmt.Print("Enter the key to find its successor: ")
            keyStr, _ := reader.ReadString('\n')
            keyStr = strings.TrimSpace(keyStr)
            key, err := strconv.Atoi(keyStr)
            if err != nil {
                fmt.Println("Invalid key")
                continue
            }
            // Find successor of key
            fmt.Printf("Successor: %d\n", peer.Successor.ID)
        case 3:
            fmt.Print("Enter the filename to take its hash: ")
            filename, _ := reader.ReadString('\n')
            filename = strings.TrimSpace(filename)
            fileID := hash(filename)
            fmt.Printf("Hash of %s: %d\n", filename, fileID)
        case 4:
            fmt.Printf("My ID: %d, Succ ID: %d, Pred ID: %d\n", peer.ID, peer.Successor.ID, peer.Predecessor.ID)
        case 5:
            fmt.Println("Stored filenames and their keys:")
            for filename, fileID := range peer.Files {
                fmt.Printf("%s: %s\n", filename, fileID)
            }
        case 6:
            fmt.Println("Exiting...")
            return
        default:
            fmt.Println("Invalid option")
        }
    }
}
