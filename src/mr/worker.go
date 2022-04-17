package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSlice []KeyValue

func (a KeyValueSlice) Len() int           { return len(a) }
func (a KeyValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, args ResponseArgs) int {
	// Extract the contents of file
	filename := args.Data
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return -1
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return -1
	}
	file.Close()

	// Execute map task
	kva := mapf(filename, string(content))
	log.Printf("%v records from map\n", len(kva))

	// Shuffle the kva data and write to intermedidate file
	sort.Sort(KeyValueSlice(kva))
	prefix := "mr-" + strconv.Itoa(args.Number) + "-"

	i := 0
	for i < len(kva) {
		// Divide by key
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		intermedidate := kva[i:j]

		// Write to file
		new_filename := prefix + strconv.Itoa(ihash(intermedidate[0].Key)%args.NReduce)
		ofile, err := os.OpenFile(new_filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot open %v", new_filename)
			return -1
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range intermedidate {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", new_filename)
				return -1
			}
		}
		ofile.Close()
		log.Printf("write %v [%v] records to %v\n",
			len(intermedidate), intermedidate[0].Key, new_filename)

		i = j
	}

	// Notify the coordinator that map task done
	// TODO: to be finished

	return 0
}

func doReduceTask(reducef func(string, []string) string, args ResponseArgs) int {
	// Wait for all map tasks finished
	// TODO: to be finished

	result := []string{}
	content := []KeyValue{}

	// For each intermedidate file in current path
	files, _ := ioutil.ReadDir("./")
	for _, f := range files {
		filename := f.Name()
		if filename[len(filename)-1] == uint8('0'+args.Number) {
			log.Printf("parsing %v", filename)

			// Read file and extract content
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return -1
			}
			temp_content := []KeyValue{}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				temp_content = append(temp_content, kv)
			}
			content = append(content, temp_content...)
			log.Printf("extract %v records from %v, %v in all",
				len(temp_content), filename, len(content))

			// Execute reduce task
			i := 0
			for i < len(content) {
				j := i + 1
				for j < len(content) && content[j].Key == content[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, content[k].Value)
				}
				output := reducef(content[i].Key, values)
				result = append(result, fmt.Sprintf("%v %v", content[i].Key, output))
				i = j
			}

			file.Close()
		}
	}

	// Write to mr-out-x
	prefix := "mr-out-"
	new_filename := prefix + strconv.Itoa(args.Number)
	file, err := os.Create(new_filename)
	if err != nil {
		log.Fatalf("cannot create %v", new_filename)
		return -1
	}
	for _, str := range result {
		file.WriteString(str + "\n")
	}
	file.Close()

	return 0
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := RequestArgs{}
	reply := ResponseArgs{}
	ok := call("Coordinator.Dispatch", &args, &reply)
	if ok {
		log.Printf("reply from cooardinator: %v\n", reply)
	} else {
		log.Printf("call failed!\n")
	}

	switch reply.Type {
	case MAP:
		log.Println("Worker execute map task...")
		ret := doMapTask(mapf, reply)
		if ret != 0 {
			log.Println("map task failed")
		}
	case REDUCE:
		log.Println("Worker execute reduce task...")
		ret := doReduceTask(reducef, reply)
		if ret != 0 {
			log.Println("reduce task failed")
		}
	default:
		log.Println("Error work type!")
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
